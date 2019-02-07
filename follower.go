package main


import (
	"os"
	"bufio"
	"fmt"
	"syscall"
	"time"
	"io"
)

const deltat=100

type follower struct {
	filename string
	inode    uint64
	currpos  int64
	f        *os.File
	b	 *bufio.Reader
	valid	bool
}

func get_inode(filename string)  (uint64,bool) {
        fileinfo,err:=os.Stat(filename)
        if err!=nil { 
                return 0, false
        }
        stat, ok := fileinfo.Sys().(*syscall.Stat_t)
        if (!ok) {
                return 0,false
                }
        return stat.Ino, true
}

func (fll *follower) init(filename string) {
	var err error
	fll.filename=filename
	fll.f,err=os.Open(fll.filename)
        if err!=nil { panic(fmt.Sprintf("Unable to open log file %s .",fll.filename)) }
	fll.inode,fll.valid=get_inode(fll.filename)
	fll.currpos,_=fll.f.Seek(0, 2) // seek_end
	fll.b=bufio.NewReader(fll.f)
}

func (fll *follower) reset() {
	var err error
	fll.f.Close()
	fll.f,err=os.Open(fll.filename)
        if err!=nil { 
		fll.valid=false
		return
	}
	fll.b=bufio.NewReader(fll.f)
	fll.inode,fll.valid=get_inode(fll.filename)
	fll.currpos=0
}

func (fll * follower) rotated() bool {
	ino,vld:=get_inode(fll.filename)
	if (vld==false) || (ino!=fll.inode) {
		return true
	}
	stat,err:=os.Stat(fll.filename)
	if err!=nil {
		return true
	}
	if (stat.Size() < fll.currpos) {
		return true
	}
	return false
}

func (fll * follower) tail() string{
	var err error
	for {
		if !fll.valid {
			time.Sleep(deltat * time.Millisecond)
			fll.reset()
			continue
		}
		fll.currpos,err=fll.f.Seek(0,1) // seek_cur
		if err!=nil {
			time.Sleep(deltat * time.Millisecond)
			fll.reset()
 			continue
		}
		line,err:=fll.b.ReadString('\n')
		if err==io.EOF {
			if fll.rotated() {
				time.Sleep(deltat * time.Millisecond)
				fll.reset()
				continue
			} else {
				time.Sleep(deltat * time.Millisecond)
				continue
			}
		} else if err!=nil {
			time.Sleep(deltat * time.Millisecond)
			fll.reset()
			continue
		} else {
			return line
		}
	}
}

