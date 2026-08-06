package lock

import (
	"time"
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	version rpc.Tversion
	lockToken string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = lockname
	lk.version = 0 
	lk.lockToken = kvtest.RandValue(8)
	return lk
}

var lockReleaseValue = "release"

func (lk *Lock) Acquire() {
	// Your code here
	for {
		curValue, curVersion, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK {
			if (curValue == lk.lockToken) {
				return   // Already held lock
			}
			if curValue != lockReleaseValue {
				time.Sleep(1*time.Second)
				continue
			}
		} else if err != rpc.ErrNoKey {
			continue
		}

		err = lk.ck.Put(lk.lockname, lk.lockToken, curVersion)
		if err == rpc.ErrNoKey {
			lk.version = 0;
			continue
		}
		if err == rpc.OK {
			lk.version = curVersion
			break;
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		curValue, curVersion, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK {
			if curValue == lockReleaseValue {
				break; // Already Released
			}
			if curValue != lk.lockToken {
				return // 不是本锁锁住的不能释放
			}
		} else if err == rpc.ErrNoKey {
			return
		} else {
			continue
		}

		err = lk.ck.Put(lk.lockname, lockReleaseValue, curVersion)
		if err == rpc.ErrNoKey {
			lk.version = 0;
			continue
		}
		if err == rpc.OK {
			lk.version = curVersion
			break;
		}
	}
}
