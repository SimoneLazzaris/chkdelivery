package main
import (
        "fmt"
	"regexp"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"flag"
	"os"
	"log/syslog"
	"github.com/coreos/go-systemd/daemon"
	"time"
)

type pOperation func([]string)

type regaction struct {
	reg *regexp.Regexp
	name string
	op pOperation
	}

type msgData struct {
	id string
	from string
	to string
	subject string
	status string
	stline string
}

var (
	xlog *syslog.Writer
	xdebug bool
	rAct []regaction
	msgMap map[string]*msgData
	db *sql.DB
)

func manyPing(pdb **sql.DB) bool {
	var err error
	err=nil
	db=*pdb
	for i:=0; i<5 && err==nil; i++ { err=db.Ping() }
	if err!=nil {
		fmt.Printf("Database lost, trying to reconnect...\n")
		db.Close()
		db,err=sql.Open("mysql",fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?autocommit=true",cfg["dbuser"],cfg["dbpass"],cfg["dbhost"],cfg["dbport"],cfg["dbname"]))
		if err!=nil {
			fmt.Printf("Reconnection error: '%s'\n",err)
			return false
		}
		db.SetConnMaxLifetime(time.Minute*3)
		fmt.Printf("Reconnected\n")
		err=db.Ping()
		if err!=nil { fmt.Printf("Ping after reopening Error: '%s'\n",err) }
		*pdb=db
	}
	var dummy int
	err=db.QueryRow("SELECT 169").Scan(&dummy)
	if err!=nil {
		fmt.Printf("Banal query failed: '%s'\n",err)
		return false
	}
	if dummy!=169 {
		fmt.Printf("Banal query wrong result: '%d'\n",dummy)
		return false
	}
	return true
}

func fConnection(parms []string) {
	msgid:=parms[4]
	if xdebug { 
		xlog.Debug(fmt.Sprintf("new connection: id %s",msgid)) 
		fmt.Printf("new connection: id %s\n",msgid)
	}
	msgMap[msgid]=&msgData{msgid,"","","","",""}
}

func fNewMessage(parms []string) {
	msgid:=parms[4]
	if xdebug { 
		xlog.Debug(fmt.Sprintf("new message %s <%s>",msgid,parms[5])) 
		fmt.Printf(fmt.Sprintf("new message %s <%s>\n",msgid,parms[5])) 
	}
	if _,ok:=msgMap[msgid]; !ok {
		// local message, no smtp connection
		msgMap[msgid]=&msgData{msgid,"","","","",""}
	}
}

func fQueueIn(parms []string) {
	msgid:=parms[4]
	msgfrom:=parms[5]
	if xdebug { 
		xlog.Debug(fmt.Sprintf("queue: Message id %s from %s",msgid,msgfrom)) 
		fmt.Printf("queue: Message id %s from %s\n",msgid,msgfrom)
	}
	if _,ok:=msgMap[msgid]; !ok {
		// bounce, not present in msgMap
		msgMap[msgid]=&msgData{msgid,"<BOUNCE>","","","",""}
	} else {
		msgMap[msgid].from=msgfrom
	}
}

func fQueueOut(parms []string) {
	msgid:=parms[4]
	if _,ok:=msgMap[msgid]; !ok {
		xlog.Err(fmt.Sprintf("queue: ERROR! Message %s not found",msgid))
		fmt.Printf("queue: ERROR! Message %s not found\n",msgid)
		return
	}
	if xdebug { 
		xlog.Debug(fmt.Sprintf("queue: Message id %s REMOVED",msgid)) 
		fmt.Printf("queue: Message id %s REMOVED\n",msgid)
	}
	delete(msgMap,msgid)
}

func fSmtp(parms []string) {
	msgid:=parms[4]
	msgto:=parms[5]
	msgstatus:=parms[6]
	msgextra:=parms[7]
	if xdebug { 
		xlog.Debug(fmt.Sprintf("smtpout: Message id %s to %s status %s [%s]",msgid,msgto,msgstatus,msgextra)) 
		fmt.Printf("smtpout: Message id %s to %s status %s [%s]\n",msgid,msgto,msgstatus,msgextra)
	}
	if _,ok:=msgMap[msgid]; !ok {
		// not present in msgMap
		msgMap[msgid]=&msgData{msgid,"<???>","","","",""}
	}
	msgMap[msgid].to=msgto
	msgMap[msgid].status=msgstatus
	msgMap[msgid].stline=msgextra
	
	if manyPing(&db) {
		_,err:=db.Exec("INSERT INTO "+cfg["dbtable"]+" (qid, tstamp, sender, recipient, status, msg, subject) VALUES (?, NOW(), ?, ?, ?, ?, ?)", 
			msgid, msgMap[msgid].from, msgMap[msgid].to, msgMap[msgid].status, msgMap[msgid].stline, msgMap[msgid].subject)
		if err!=nil {
			xlog.Err(fmt.Sprintf("%s",err))
			fmt.Printf("ERROR: %s\n",err)
		} else {
			if xdebug { 
				xlog.Debug("smtpout: inserted")
				fmt.Printf("smtpout: inserted\n")
			}
		}
	} else {
		xlog.Err("Unable to use MySQL")
		fmt.Printf("Unable to use MySQL")
	}
}

func fRspamd(parms []string) {
	msgid:=parms[4]
	subject:=parms[5]
	if _,ok:=msgMap[msgid]; !ok {
		xlog.Debug(fmt.Sprintf("Message %s not found",msgid))
		fmt.Printf("Message %s not found\n",msgid)
		return
	}
	msgMap[msgid].subject=subject
}

func init() {
	xlog, _ = syslog.New(syslog.LOG_INFO|syslog.LOG_DAEMON, "chkdelivery")
	cfgfile:=flag.String("cfg","/usr/local/etc/chkdelivery.cf","Configuration file name")
	flag.BoolVar(&xdebug,"debug",false,"Enable debug messages")
	flag.Parse()
	InitCfg(*cfgfile)
	rAct=[]regaction{
 		regaction {regexp.MustCompile(`^(... .. \d\d:\d\d:\d\d) (\S+) (postfix/smtpd)\[\d+\]: (\S+): (.*)`),"connection",fConnection}, 
 		regaction {regexp.MustCompile(`^(... .. \d\d:\d\d:\d\d) (\S+) (postfix/cleanup)\[\d+\]: (\S+): message-id=<(\S*)>`),"newmessage",fNewMessage}, 
 		regaction {regexp.MustCompile(`^(... .. \d\d:\d\d:\d\d) (\S+) (postfix/qmgr)\[\d+\]: (\S+): from=<(\S*)>, size=(\d+), (.*)`), "queuemanager",fQueueIn}, 
 		regaction {regexp.MustCompile(`^(... .. \d\d:\d\d:\d\d) (\S+) (postfix/smtp)\[\d+\]: (\S+): to=<(\S*)>, .* status=(\S+) (.*)`), "smtpout",fSmtp},
 		regaction {regexp.MustCompile(`^(... .. \d\d:\d\d:\d\d) (\S+) (postfix/qmgr)\[\d+\]: (\S+): (removed)`), "queuemanager",fQueueOut}, 
 		regaction {regexp.MustCompile(`^(... .. \d\d:\d\d:\d\d) (\S+) (rspamd)\[\d+\]: .*, qid: <(\S+)>, .*, subject: "(.*)"`), "rspamd", fRspamd},
	}
	msgMap=make(map[string]*msgData)
	var err error
        db,err=sql.Open("mysql",fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?autocommit=true",cfg["dbuser"],cfg["dbpass"],cfg["dbhost"],cfg["dbport"],cfg["dbname"]))
        if (err!=nil) {
                fmt.Println("ERROR CONNECTING MYSQL")
		xlog.Crit("ERROR CONNECTING MYSQL")
                os.Exit(1)
        }
}

func main() {
	ff:=follower{}
	ff.init(cfg["logfile"])
	daemon.SdNotify(false, "READY=1")
	if xdebug { 
		xlog.Debug(fmt.Sprintf("chkdelivery: starting")) 
		fmt.Println("chkdelivery: starting")
	}
	for {
		s:=ff.tail()
		for _,v:=range rAct {
			f:=v.reg.FindStringSubmatch(s)
			if f==nil { continue }
			v.op(f)
		}
	}
}
