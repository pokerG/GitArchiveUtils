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
	"flag"
	"sync"
	//"sync/atomic"
	"time"
	//"labix.org/v2/mgo/bson"
)

const BUFSIZE int = 40000
var CHANNUM int 
var NUMINSERT int 

var chs []chan int
var cnum int
var dcnum int

func main() {
		runtime.GOMAXPROCS(runtime.NumCPU())
		cn := flag.Int("c",24,"number of channel")
		in := flag.Int("i",7000,"number of insert")
		flag.Parse()
		CHANNUM = *cn
		NUMINSERT = *in
		
		args := flag.Args()
		if len(args) > 0{
			t := time.Now()
			fmt.Println(CHANNUM,NUMINSERT)
		chs = make([]chan int, CHANNUM)
		cnum = 0
		for i, _ := range chs {
			chs[i] = make(chan int)
		}
		Tree(args[0])
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
			for _, ch := range chs {
				<-ch
			}
			cnum = 0
			fmt.Println("One luan")
			time.Sleep(time.Second * 5)
			runtime.Gosched()
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

	for {
		n, err := gr.Read(buf)
		data = append(data, buf[:n]...)
		if err == io.EOF {
			break
		}
		num += n
		handleError(err)
	}

	WriteToMongo(fr.Name(), data, ch)
}

func WriteToMongo(fname string, data []byte, ch chan int) {
	session, err := mgo.Dial("172.16.0.1:27017")
	handleError(err)
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	reg, err := regexp.Compile(`[{].*[}][\n]`)
	handleError(err)

	c := session.DB("testGoBig").C("Event")
	sdata := reg.FindAllString(string(data), -1)

	inter := make([]interface{}, len(sdata))
	for i, s := range sdata {
		err := json.Unmarshal([]byte(s), &inter[i])
		handleError(err)
	}
	var i int
	for i = 0; i < len(inter)/NUMINSERT; i++ {
		c.Insert(inter[i*NUMINSERT : (i+1)*NUMINSERT])
	}
	c.Insert(inter[i*NUMINSERT:])

	lock := &sync.Mutex{}
	lock.Lock()
	fmt.Println(fname)
	fmt.Println(len(sdata))
	handleError(err)
	ch <- 1
	lock.Unlock()
}

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
