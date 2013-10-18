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
	"time"
	//"sync"
	//"labix.org/v2/mgo/bson"
)

const BUFSIZE int = 40000

var chs []chan int
var cnum int
var chtmp chan int

func main() {
	if len(os.Args) > 1 {
		runtime.GOMAXPROCS(runtime.NumCPU())

		t := time.Now()

		chs = make([]chan int, 1)
		cnum = 0

		Tree(os.Args[1], 1)
		for _, ch := range chs {
			<-ch
		}
		fmt.Println("total time :", time.Since(t))

	} else {
		fmt.Println("Please input the Dir or file path")
	}

}

func Tree(dirname string, curHier int) {
	dirAbs, err := filepath.Abs(dirname)
	handleError(err)
	fileInfos, err := ioutil.ReadDir(dirAbs)
	handleError(err)

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			Tree(filepath.Join(dirAbs, fileInfo.Name()), curHier+1)
		} else {
			b := []byte(fileInfo.Name())
			matched, _ := regexp.Match("[.](json.gz)$", b)
			if matched {
				if cnum >= len(chs) {
					chs = append(chs, make(chan int))
				}
				chs[cnum] = make(chan int)
				go UZip(filepath.Join(dirAbs, fileInfo.Name()), chs[cnum])
				cnum += 1

			}

		}

	}
}

func UZip(fpath string, ch chan int) {
	fr, err := os.Open(fpath)
	handleError(err)
	defer fr.Close()

	fmt.Println(fr.Name())

	gr, err := gzip.NewReader(fr)
	handleError(err)

	buf := make([]byte, BUFSIZE)

	var data []byte

	var num int = 0

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

	WriteToMongo(data, ch)
}

func WriteToMongo(data []byte, ch chan int) {
	session, err := mgo.Dial("localhost:27017")
	handleError(err)
	defer session.Close()
	//fmt.Println(data)
	session.SetMode(mgo.Monotonic, true)

	reg, err := regexp.Compile(`[{].*[}][\n]`)
	handleError(err)

	sdata := reg.FindAllString(string(data), -1)
	fmt.Println(len(sdata))
	//fmt.Println(sdata)

	for _, s := range sdata {
		var inter interface{}

		err = json.Unmarshal([]byte(s), &inter)
		handleError(err)
		c := session.DB("testGoBig").C("Event")
		err = c.Insert(inter)

	}

	handleError(err)

	ch <- 1

}

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
