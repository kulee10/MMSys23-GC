// Copyright (c) 2023. ByteDance Inc. All rights reserved.
 
#pragma once
 
#include <vector>
#include <set>
#include "basefw/base/log.h"
#include "multipathschedulerI.h"
#include <numeric>
 
//---------------
// Different between std::map & std::multimap
// map only allow unique key
// multimap allow same key
// when access multimap using a key, it will return a list of values
 
//---------------
 
//------------------------------------
// parameter usage explain
 
// std::map<fw::ID, std::set<DataNumber>> m_session_needdownloadpieceQ;
// This map save the current session that this process is connected
// m_session_needdownloadpieceQ[SESSION_ID] = DOWNLOAD_TASK_SET
 
// std::set<DataNumber>& m_downloadQueue; // main task queue
// This set save all the "unarrange" & undone task ()
 
// std::multimap<Duration, fw::shared_ptr<SessionStreamController>> m_sortmmap;
// Save the session with key:duration, because it may have two or more session have same duration so they use multimap
// m_sortmmap[DURATION] = POINTER_TO_SESSION
 
// std::map<fw::ID, fw::shared_ptr<SessionStreamController>>& m_dlsessionmap;
// save the session by ID, this is a unique key
// m_dlsessionmap[SESSION_ID] = POINTER_TO_SESSION
 
//------------------------------------
 
/// min RTT Round Robin multipath scheduler
class RRMultiPathScheduler : public MultiPathSchedulerAlgo
{
public:
    MultiPathSchedulerType SchedulerType() override
    {
        return MultiPathSchedulerType::MULTI_PATH_SCHEDULE_RR;
    }
 
    explicit RRMultiPathScheduler(const fw::ID &taskid,
                                  std::map<fw::ID, fw::shared_ptr<SessionStreamController>> &dlsessionmap,
                                  std::set<DataNumber> &downloadQueue, std::set<int32_t> &lostPiecesQueue)
        : MultiPathSchedulerAlgo(taskid, dlsessionmap, downloadQueue, lostPiecesQueue)
    {
        SPDLOG_DEBUG("taskid :{}", taskid.ToLogStr());
    }
 
    ~RRMultiPathScheduler() override
    {
        SPDLOG_TRACE("");
    }
 
    int32_t StartMultiPathScheduler(fw::weak_ptr<MultiPathSchedulerHandler> mpsHandler) override
    {
        SPDLOG_DEBUG("");
        m_phandler = std::move(mpsHandler);
        return 0;
    }
 
    bool StopMultiPathScheduler() override
    {
        SPDLOG_DEBUG("");
        OnResetDownload();
        return true;
    }
 
    void OnSessionCreate(const fw::ID &sessionid) override
    {
        SPDLOG_DEBUG("session: {}", sessionid.ToLogStr());
        auto &&itor = m_session_needdownloadpieceQ.find(sessionid);
        if (itor != m_session_needdownloadpieceQ.end())
        { // found, clear the sending queue
            SPDLOG_WARN("Session: {} is already created", sessionid.ToLogStr());
            for (auto &&subpiecetask : itor->second)
            {
                m_downloadQueue.emplace(subpiecetask);
            }
        }
        m_session_needdownloadpieceQ[sessionid] = std::set<int32_t>();
    }
 
    void OnSessionDestory(const fw::ID &sessionid) override
    { // 如果有個連線被切斷了的話
        SPDLOG_DEBUG("session: {}", sessionid.ToLogStr());
        // find the session's queue, clear the subpieces and add the subpieces to main downloading queue
        auto &&itor = m_session_needdownloadpieceQ.find(sessionid);
        if (itor == m_session_needdownloadpieceQ.end())
        {
            SPDLOG_WARN("Session: {} isn't in session queue", sessionid.ToLogStr());
            return;
        }
        // second表這個map的value，也就是這個session之前負責download的東西，因為這個session斷了，所以我把他負責download的
        // 東西丟回main_download_queue
        for (auto &&subpiecetask : itor->second)
        {
            m_downloadQueue.emplace(subpiecetask);
        }
        // 因為這個session斷了，移出session queue
        m_session_needdownloadpieceQ.erase(itor);
    }
 
    void OnResetDownload() override
    { // This function is to kill all the already connected sessions in m_session_needdownloadpieceQ
        SPDLOG_DEBUG("");
 
        if (m_session_needdownloadpieceQ.empty())
        {
            return;
        }
 
        // if there are sessions in the queue
        for (auto &it_sn : m_session_needdownloadpieceQ)
        {
            if (!it_sn.second.empty())
            {
                // we need to paste all the undone task in each session back to m_downloadQueue
                m_downloadQueue.insert(it_sn.second.begin(), it_sn.second.end());
 
                it_sn.second.clear();
            }
        }
        m_session_needdownloadpieceQ.clear();
    }
 
    void DoMultiPathSchedule() override
    {
        if (m_session_needdownloadpieceQ.empty())
        {
            SPDLOG_DEBUG("Empty session map");
            return;
        }
        // sort session first
        SPDLOG_TRACE("DoMultiPathSchedule");
        // sort the session by the duration of each session
        SortSession(m_sortmmap);
 
        // send pkt requests on each session based on ascend order;
        FillUpSessionTask(); // assign download task to each session
    }
 
    uint32_t DoSinglePathSchedule(const fw::ID &sessionid) override
    {
        SPDLOG_DEBUG("session:{}", sessionid.ToLogStr());
        // if key doesn't map to a valid set, []operator should create an empty set for us
        auto &session = m_dlsessionmap[sessionid];
        if (!session)
        {
            SPDLOG_WARN("Unknown session: {}", sessionid.ToLogStr());
            return -1;
        }
 
        // Looks like get how many packets (bandwidth) this session can provide to download
        auto uni32DataReqCnt = session->CanRequestPktCnt(); // this is bandwidth
        SPDLOG_DEBUG("Free Wnd : {}", uni32DataReqCnt);
 
        // if bandwidth == 0, this session cannot send more packet, so simply return
        if (uni32DataReqCnt == 0)
        {
            SPDLOG_WARN("Free Wnd equals to 0");
            return -1;
        }
 
        // Looks like if the bandwidth > download_task, try to ask for more download_task
        if (m_downloadQueue.size() < uni32DataReqCnt)
        {
            // This lock() will create a shared_ptr that points to the same object
            auto handler = m_phandler.lock();
            if (handler)
            {
                handler->OnRequestDownloadPieces(uni32DataReqCnt - m_downloadQueue.size()); // ask for more subpieces
            }
            else
            {
                SPDLOG_ERROR("handler = null");
            }
        }
 
        /// Add task to session task queue
        std::vector<int32_t> vecSubpieceNums;
        // eject uni32DataReqCnt number of subpieces from
        for (auto itr = m_downloadQueue.begin(); itr != m_downloadQueue.end() && uni32DataReqCnt > 0;)
        {
            vecSubpieceNums.push_back(*itr);
            m_downloadQueue.erase(itr++);
            --uni32DataReqCnt;
        }
 
        // assign download task to the specific session
        m_session_needdownloadpieceQ[sessionid].insert(vecSubpieceNums.begin(), vecSubpieceNums.end());
 
        ////////////////////////////////////DoSendRequest
        DoSendSessionSubTask(sessionid);
        return 0;
    }
 
    void OnTimedOut(const fw::ID &sessionid, const std::vector<int32_t> &pns) override
    { // A download task in specific session is timeout, we will send this timeout task back to m_lostPiecesQueue
        SPDLOG_DEBUG("session {},lost pieces {}", sessionid.ToLogStr(), pns);
        for (auto &pidx : pns)
        {
            auto &&itor_pair = m_lostPiecesQueue.emplace(pidx);
            if (!itor_pair.second)
            {
                SPDLOG_WARN(" pieceId {} already marked lost", pidx);
            }
        }
    }
 
    void OnReceiveSubpieceData(const fw::ID &sessionid, SeqNumber seq, DataNumber pno, Timepoint recvtime) override
    {
        SPDLOG_DEBUG("session:{}, seq:{}, pno:{}, recvtime:{}",
                     sessionid.ToLogStr(), seq, pno, recvtime.ToDebuggingValue());
        /// rx and tx signal are forwarded directly from transport controller to session controller
 
        DoSinglePathSchedule(sessionid);
    }
 
    void SortSession(std::multimap<Duration, fw::shared_ptr<SessionStreamController>> &sortmmap) override
    { // sort section by duration
        SPDLOG_TRACE("");
        sortmmap.clear();
        for (auto &&sessionItor : m_dlsessionmap)
        {
            // this score seems to be kind of duration
            auto score = sessionItor.second->GetRtt();
            sortmmap.emplace(score, sessionItor.second);
        }
    }
 
private:
    int32_t DoSendSessionSubTask(const fw::ID &sessionid) override
    { // This function is to start to download request of the session, it will check the current bandwidth
      // this session have, and start the download task that former task manager assign to it
 
        SPDLOG_TRACE("session id: {}", sessionid.ToLogStr());
        int32_t i32Result = -1;
 
        // get the download task set this session have
        auto &setNeedDlSubpiece = m_session_needdownloadpieceQ[sessionid];
        if (setNeedDlSubpiece.empty())
        {
            SPDLOG_TRACE("empty sending queue");
            return i32Result;
        }
 
        // get section pointer of this session id
        auto &session = m_dlsessionmap[sessionid];
 
        // Looks like get the bandwidth this section left
        uint32_t u32CanSendCnt = session->CanRequestPktCnt();
        std::vector<int32_t> vecSubpieces;
 
        // According to the Bandwidth we currently have, assign the task in setNeedDlSubpiece to vecSubpieces.
        // vecSubpieces will then start request
        for (auto itor = setNeedDlSubpiece.begin(); itor != setNeedDlSubpiece.end() && vecSubpieces.size() < u32CanSendCnt;)
        {
            vecSubpieces.emplace_back(*itor);
            setNeedDlSubpiece.erase(itor++);
        }
 
        // Let download tasks in vecSubpieces start request
        bool rt = m_dlsessionmap[sessionid]->DoRequestdata(sessionid, vecSubpieces);
        if (rt)
        {
            i32Result = 0;
            // succeed
        }
        else
        {
            // fail
            // return sending pieces to main download queue
            SPDLOG_DEBUG("Send failed, Given back");
            m_downloadQueue.insert(setNeedDlSubpiece.begin(), setNeedDlSubpiece.end());
        }
 
        return i32Result;
    }
 
    void FillUpSessionTask()
    {
        // 1. put lost packets back into main download queue
        SPDLOG_TRACE("");
        for (auto &&lostpiece : m_lostPiecesQueue)
        {
            auto &&itor_pair = m_downloadQueue.emplace(lostpiece);
            if (itor_pair.second)
            {
                SPDLOG_TRACE("lost piece {} inserts successfully", lostpiece);
            }
            else
            {
                SPDLOG_TRACE("lost piece {} already in task queue", lostpiece);
            }
        }
        m_lostPiecesQueue.clear();
 
        // 2. go through every session,find how many pieces we can request at one time
        std::map<basefw::ID, uint32_t> toSendinEachSession; // list of bandwith of each session
        for (auto &&itor : m_dlsessionmap)
        {
            auto &sessionId = itor.first;
            auto &sessStream = itor.second;
            auto sessCanSendCnt = sessStream->CanRequestPktCnt();
            toSendinEachSession.emplace(sessionId, sessCanSendCnt);
            if (sessCanSendCnt != 0)
            {
                SPDLOG_TRACE("session {} has {} free wnd", sessionId.ToLogStr(), sessCanSendCnt);
            }
        }
        uint32_t totalSubpieceCnt = std::accumulate(toSendinEachSession.begin(), toSendinEachSession.end(),
                                                    0, [](size_t total, std::pair<const basefw::ID, uint32_t> &session_task_itor)
                                                    { return total + session_task_itor.second; });
 
        // 3. try to request enough piece cnt from up layer, if necessary
        if (m_downloadQueue.size() < totalSubpieceCnt)
        {
            auto handler = m_phandler.lock();
            if (handler)
            {
                handler->OnRequestDownloadPieces(totalSubpieceCnt - m_downloadQueue.size());
            }
            else
            {
                SPDLOG_ERROR("handler = null");
            }
        }
 
        SPDLOG_TRACE(" download queue size: {}, need pieces cnt: {}", m_downloadQueue.size(), totalSubpieceCnt);
 
        // 4. fill up each session Queue, based on min RTT first order, and send
        // RTT fist order should be Duration
 
        std::multimap<Duration, uint32_t> duration_tasknum_map; // <rtt, bw>
        std::vector<DataNumber> vecToSendpieceNums; // download task vector
        for (auto &&rtt_sess : m_sortmmap)
        {
            auto curr_duration = rtt_sess.first;
            auto &sessStream = rtt_sess.second;
            auto &&sessId = sessStream->GetSessionId();
            auto &&itor_id_ssQ = m_session_needdownloadpieceQ.find(sessId);
            if (itor_id_ssQ != m_session_needdownloadpieceQ.end())
            {
                // find the bandwidth of this session_ID
                auto &&id_sendcnt = toSendinEachSession.find(sessId);
                if (id_sendcnt != toSendinEachSession.end())
                {
                    auto uni32DataReqCnt = toSendinEachSession.at(sessId);
                    // assign download tasks from m_downloadQueue while bandwidth is enough
                    auto &&itr = m_downloadQueue.begin();
                    for (auto &&duration_tasknum : duration_tasknum_map)
                    {
                        int former_duration = int(duration_tasknum.first.ToMilliseconds());
                        int int_curr_duration = int(curr_duration.ToMilliseconds());
                        double times = double(int_curr_duration) / double(former_duration) - 1;
                        if (times >= 1)
                        {
                            int bandwidth = int(duration_tasknum.second);
                            for (int offset = 0; offset < int(times) * bandwidth && itr != m_downloadQueue.end(); offset++)
                            {
                                itr++;
                            }
                            if (itr == m_downloadQueue.end())
                            {
                                break;
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
 
                    for (; itr != m_downloadQueue.end() && uni32DataReqCnt > 0;)
                    {
 
                        vecToSendpieceNums.push_back(*itr);
                        m_downloadQueue.erase(itr++); // remove task from main tasks manager
                        --uni32DataReqCnt;
                    }
                    // add the assign tasks to this session_id's task set
                    m_session_needdownloadpieceQ[sessId].insert(vecToSendpieceNums.begin(),
                                                                vecToSendpieceNums.end());
                    vecToSendpieceNums.clear();
                    duration_tasknum_map.emplace(curr_duration, uni32DataReqCnt);
                }
                else
                {
                    SPDLOG_ERROR("Can't found session {} in toSendinEachSession", sessId.ToLogStr());
                }
            }
            else
            {
                SPDLOG_ERROR("Can't found Session:{} in session_needdownloadsubpiece", sessId.ToLogStr());
            }
        }
 
        // then send in each session
        for (auto &&it_sn = m_session_needdownloadpieceQ.begin();
             it_sn != m_session_needdownloadpieceQ.end(); ++it_sn)
        {
            auto &sessId = it_sn->first;
            auto &sessQueue = it_sn->second;
            SPDLOG_TRACE("session Id:{}, session queue:{}", sessId.ToLogStr(), sessQueue);
            DoSendSessionSubTask(it_sn->first);
        }
 
    } // end of FillUpSessionTask
 
    /// It's multipath scheduler's duty to maintain session_needdownloadsubpiece, and m_sortmmap
    std::map<fw::ID, std::set<DataNumber>> m_session_needdownloadpieceQ; // session task queues
    std::multimap<Duration, fw::shared_ptr<SessionStreamController>> m_sortmmap;
    fw::weak_ptr<MultiPathSchedulerHandler> m_phandler;
};