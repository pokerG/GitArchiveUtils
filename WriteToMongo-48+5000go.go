package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	//"sync/atomic"
	"time"
	//"labix.org/v2/mgo/bson"
)

const BUFSIZE int = 40000
const CHANNUM int = 48
const DCHANNUM int = 6000

var chs []chan int
var cnum int
var dcnum int

func main() {
	if len(os.Args) > 1 {
		runtime.GOMAXPROCS(runtime.NumCPU())

		t := time.Now()

		chs = make([]chan int, CHANNUM+DCHANNUM)
		cnum = 0
		for i, _ := range chs {
			chs[i] = make(chan int)
		}
		Tree(os.Args[1])
		fmt.Println("total time :", time.Since(t))

	} else {
		fmt.Println("Please input the Dir or file path")
	}

}

func Tree(dirname string) {
	dirAbs, err := filepath.Abs(dirname)
	handleError(err)
	fileInfos, err := ioutil.ReadDir(dirAbs)
	handleError(err)

	for i, fileInfo := range fileInfos {
		if cnum == CHANNUM {
			for _, ch := range chs[:CHANNUM] {
				<-ch
			}
			cnum = 0
			fmt.Println("One luan")
		}
		go UZip(filepath.Join(dirAbs, fileInfo.Name()), chs[cnum])
		if i == len(fileInfos)-1 {
			for i := 0; i <= cnum; i++ {
				<-chs[i]
			}
		}
		cnum += 1

	}
}

func UZip(fpath string, ch chan int) {
	fr, err := os.Open(fpath)
	handleError(err)
	defer fr.Close()

	//fmt.Println(fr.Name())

	gr, err := gzip.NewReader(fr)
	handleError(err)

	buf := make([]byte, BUFSIZE)

	var data []byte

	var num int = 0

	t := time.Now()
	for {

		n, err := gr.Read(buf)
		//fmt.Println(n)
		//fmt.Println(buf)
		data = append(data, buf[:n]...)

		if err == io.EOF {
			break
		}
		num += n
		handleError(err)
	}
	fmt.Println("Read file:", time.Since(t))

	WriteToMongo(fr.Name(), data, ch)
}

func WriteToMongo(fname string, data []byte, ch chan int) {
	session, err := mgo.Dial("localhost:27017")
	handleError(err)
	defer session.Close()
	//fmt.Println(data)
	session.SetMode(mgo.Monotonic, true)

	t := time.Now()
	reg, err := regexp.Compile(`[{].*[}][\n]`)
	fmt.Println("regexp :", time.Since(t))
	handleError(err)

	c := session.DB("testGoBig").C("Event")
	sdata := reg.FindAllString(string(data), -1)

	t = time.Now()

	for i, s := range sdata {
		if dcnum == DCHANNUM {
			for _, dch := range chs[CHANNUM:] {
				<-dch
			}
			dcnum = 0
		}
		go func(s string, ch chan int) {
			var inter interface{}

			err = json.Unmarshal([]byte(s), &inter)
			handleError(err)
			err = c.Insert(inter)
			ch <- 1
		}(s, chs[CHANNUM+dcnum])
		if i == len(sdata)-1 {
			for i := 0; i <= dcnum; i++ {
				<-chs[CHANNUM+i]
			}
		}
		dcnum += 1

	}

	fmt.Println("Write db:", time.Since(t))

	lock := &sync.Mutex{}
	lock.Lock()
	fmt.Println(fname)
	fmt.Println(len(sdata))
	//atomic.AddInt32(&currentNum, -1)
	handleError(err)
	ch <- 1
	lock.Unlock()
}

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
