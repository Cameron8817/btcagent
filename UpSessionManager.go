package main

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/golang/glog"
)

type UpSessionInfo struct {
	minerNum  int
	ready     bool
	upSession UpSession
}

type FakeUpSessionInfo struct {
	minerNum  int
	upSession FakeUpSession
}

type UpSessionManager struct {
	id string // 打印日志用的连接标识符

	subAccount string
	config     *Config
	parent     *SessionManager

	upSessions    []UpSessionInfo
	fakeUpSession FakeUpSessionInfo

	eventChannel chan interface{}

	initSuccess        bool
	initFailureCounter int

	printingMinerNum bool

	// CHANGED: IP filter for extra miners
	extraRanges []IPRange
}

func NewUpSessionManager(subAccount string, config *Config, parent *SessionManager) (manager *UpSessionManager) {
	manager = new(UpSessionManager)
	manager.subAccount = subAccount
	manager.config = config
	manager.parent = parent

	// parse filter for extra miners
	manager.extraRanges = parseRange(BTCExtraFilter)

	var upSessions []UpSessionInfo;
	if manager.config.AgentType == "btc" {
		// CHANGED: add more sessions for extra BTC pool
		// 0..PoolConnectionNumberPerSubAccount -> original pool defined in agent_conf.json
		// PoolConnectionNumberPerSubAccount.. -> extra pool defined in Const.go
		upSessions = make([]UpSessionInfo, manager.config.Advanced.PoolConnectionNumberPerSubAccount * 2)
	} else {
		upSessions = make([]UpSessionInfo, manager.config.Advanced.PoolConnectionNumberPerSubAccount)
	}
	manager.upSessions = upSessions[:]
	manager.fakeUpSession.upSession = manager.config.sessionFactory.NewFakeUpSession(manager)

	manager.eventChannel = make(chan interface{}, manager.config.Advanced.MessageQueueSize.PoolSessionManager)

	if manager.config.MultiUserMode {
		manager.id = fmt.Sprintf("<%s> ", manager.subAccount)
	}
	return
}

func (manager *UpSessionManager) Run() {
	go manager.fakeUpSession.upSession.Run()

	for i := range manager.upSessions {
		go manager.connect(i)
	}

	manager.handleEvent()
}

// CHANGED: add extra slots to extra pool
func (manager *UpSessionManager) connect(slot int) {
	if slot > int(manager.config.Advanced.PoolConnectionNumberPerSubAccount) {
		// Extra BTC pool
		for i := range BTCExtraPools {
			up := manager.config.sessionFactory.NewUpSession(manager, len(manager.config.Pools) + i, slot)
			up.Init()
	
			if up.Stat() == StatAuthorized {
				go up.Run()
				manager.SendEvent(EventUpSessionReady{slot, up})
				return
			}
		}
		manager.SendEvent(EventUpSessionInitFailed{slot})
		return 
	}
	for i := range manager.config.Pools {
		up := manager.config.sessionFactory.NewUpSession(manager, i, slot)
		up.Init()

		if up.Stat() == StatAuthorized {
			go up.Run()
			manager.SendEvent(EventUpSessionReady{slot, up})
			return
		}
	}
	manager.SendEvent(EventUpSessionInitFailed{slot})
}

func (manager *UpSessionManager) SendEvent(event interface{}) {
	manager.eventChannel <- event
}

// CHANGED: check downsession ip address & assign extra pool if needed
func (manager *UpSessionManager) addDownSession(e EventAddDownSession) {
	defer manager.tryPrintMinerNum()

	var isExtraMiner = false
	if manager.config.AgentType == "btc" {
		sess, _ := e.Session.(*DownSessionBTC)
		glog.Info(sess.id, "is connecting to pool")

		var ip net.IP
		dotPos := strings.IndexByte(sess.fullName, '.')
		if dotPos >= 0 {
			ipStr := sess.fullName[dotPos + 1:]
			ipStr = strings.Replace(ipStr, "x", ".", -1)
			ip = net.ParseIP(ipStr)

			if ip != nil {
				glog.Info("Parsed ip address of ", sess.id, "is ", ip)
			}
		}

		if ip == nil {
			// failed to parse address from full name
			// set ip address to remote address of client connection
			ip = net.ParseIP(sess.clientConn.RemoteAddr().String())
			glog.Info("Failed to parse ip address of ", sess.id, "- setting from remoteAddr() function ", ip)
		}

		if find(manager.extraRanges, ip) {
			isExtraMiner = true
			glog.Info(sess.id, "is need to connect to Pool B")
		}
	}

	// check apply deadline of extra pool
	deadline, _ := time.Parse(time.RFC1123, BTCExtraPoolApplyDeadline)
	if time.Now().After(deadline) {
		isExtraMiner = false
		glog.Info("Trial version is expired. So all pool B miners are set to connect pool A")
	}

	var selected *UpSessionInfo

	// 寻找连接数最少的服务器
	for i := range manager.upSessions {
		if isExtraMiner {
			// extra miner - skip original pools
			if i < len(manager.config.Pools) {
				continue
			}
		} else {
			// original miner - skip extra pools
			if i >= len(manager.config.Pools){
				continue
			}
		}
		info := &manager.upSessions[i]
		if info.ready && (selected == nil || info.minerNum < selected.minerNum) {
			selected = info
		}
	}

	if selected != nil {
		selected.minerNum++
		e.Session.SendEvent(EventSetUpSession{selected.upSession})
		return
	}

	// 服务器均未就绪，若已启用 AlwaysKeepDownconn，就把矿机托管给 FakeUpSession
	if manager.config.AlwaysKeepDownconn {
		manager.fakeUpSession.minerNum++
		e.Session.SendEvent(EventSetUpSession{manager.fakeUpSession.upSession})
		return
	}

	// 未启用 AlwaysKeepDownconn，直接断开连接，防止矿机认为 BTCAgent 连接活跃
	e.Session.SendEvent(EventPoolNotReady{})
}

func (manager *UpSessionManager) upSessionReady(e EventUpSessionReady) {
	defer manager.tryPrintMinerNum()

	manager.initSuccess = true

	info := &manager.upSessions[e.Slot]
	info.upSession = e.Session
	info.ready = true

	// 从 FakeUpSession 拿回矿机
	manager.fakeUpSession.upSession.SendEvent(EventTransferDownSessions{})
}

func (manager *UpSessionManager) upSessionInitFailed(e EventUpSessionInitFailed) {
	if manager.initSuccess {
		glog.Error(manager.id, "Failed to connect to all ", len(manager.config.Pools), " pool servers, please check your configuration! Retry in 5 seconds.")
		go func() {
			time.Sleep(5 * time.Second)
			manager.connect(e.Slot)
		}()
		return
	}

	manager.initFailureCounter++

	if manager.initFailureCounter >= len(manager.upSessions) {
		glog.Error(manager.id, "Too many connection failure to pool, please check your sub-account or pool configurations! Sub-account: ", manager.subAccount, ", pools: ", manager.config.Pools)

		manager.parent.SendEvent(EventStopUpSessionManager{manager.subAccount})
		return
	}
}

func (manager *UpSessionManager) upSessionBroken(e EventUpSessionBroken) {
	defer manager.tryPrintMinerNum()

	info := &manager.upSessions[e.Slot]
	info.ready = false
	info.minerNum = 0

	go manager.connect(e.Slot)
}

func (manager *UpSessionManager) updateMinerNum(e EventUpdateMinerNum) {
	defer manager.tryPrintMinerNum()

	manager.upSessions[e.Slot].minerNum -= e.DisconnectedMinerCounter

	if glog.V(3) {
		glog.Info(manager.id, "miner num update, slot: ", e.Slot, ", miners: ", manager.upSessions[e.Slot].minerNum)
	}

	if manager.config.MultiUserMode {
		minerNum := 0
		for i := range manager.upSessions {
			minerNum += manager.upSessions[i].minerNum
		}
		if minerNum < 1 {
			glog.Info(manager.id, "no miners on sub-account ", manager.subAccount, ", close pool connections")
			manager.parent.SendEvent(EventStopUpSessionManager{manager.subAccount})
		}
	}
}

func (manager *UpSessionManager) updateFakeMinerNum(e EventUpdateFakeMinerNum) {
	defer manager.tryPrintMinerNum()

	manager.fakeUpSession.minerNum -= e.DisconnectedMinerCounter
}

func (manager *UpSessionManager) updateFakeJob(e interface{}) {
	manager.fakeUpSession.upSession.SendEvent(e)
}

func (manager *UpSessionManager) exit() {
	manager.fakeUpSession.upSession.SendEvent(EventExit{})

	for _, up := range manager.upSessions {
		if up.ready {
			up.upSession.SendEvent(EventExit{})
		}
	}
}

func (manager *UpSessionManager) tryPrintMinerNum() {
	if manager.printingMinerNum {
		return
	}
	manager.printingMinerNum = true
	go func() {
		time.Sleep(5 * time.Second)
		manager.SendEvent(EventPrintMinerNum{})
	}()
}

func (manager *UpSessionManager) printMinerNum() {
	pools := 0
	miners := manager.fakeUpSession.minerNum
	for _, info := range manager.upSessions {
		miners += info.minerNum
		if info.ready {
			pools++
		}
	}
	glog.Info(manager.id, "connection number changed, pool servers: ", pools, ", miners: ", miners)
	manager.printingMinerNum = false
}

func (manager *UpSessionManager) handleEvent() {
	for {
		event := <-manager.eventChannel

		switch e := event.(type) {
		case EventUpSessionReady:
			manager.upSessionReady(e)
		case EventUpSessionInitFailed:
			manager.upSessionInitFailed(e)
		case EventAddDownSession:
			manager.addDownSession(e)
		case EventUpSessionBroken:
			manager.upSessionBroken(e)
		case EventUpdateMinerNum:
			manager.updateMinerNum(e)
		case EventUpdateFakeMinerNum:
			manager.updateFakeMinerNum(e)
		case EventUpdateFakeJobBTC:
			manager.updateFakeJob(e)
		case EventUpdateFakeJobETH:
			manager.updateFakeJob(e)
		case EventPrintMinerNum:
			manager.printMinerNum()
		case EventExit:
			manager.exit()
			return
		default:
			glog.Error("[UpSessionManager] unknown event: ", event)
		}
	}
}
