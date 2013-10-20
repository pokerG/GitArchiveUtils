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
	//"sync"
	//"sync/atomic"
	"time"
	//"labix.org/v2/mgo/bson"
)

const BUFSIZE int = 40000

var chs []chan int

func main() {
	if len(os.Args) > 1 {
		runtime.GOMAXPROCS(runtime.NumCPU())
		t := time.Now()
		Tree(os.Args[1], 1)
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
				UZip(filepath.Join(dirAbs, fileInfo.Name()))
			}
		}
	}
}

func UZip(fpath string) {

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

	WriteToMongo(data)
	fmt.Println("finish!!")
}

func WriteToMongo(data []byte) {
	session, err := mgo.Dial("localhost:27017")
	handleError(err)
	defer session.Close()
	//fmt.Println(data)
	session.SetMode(mgo.Monotonic, true)

	c := session.DB("testGoBig").C("Event")

	reg, err := regexp.Compile(`[{].*[}][\n]`)
	handleError(err)

	sdata := reg.FindAllString(string(data), -1)

	chs = make([]chan int, len(sdata)/1000+1)

	for i, _ := range chs {
		chs[i] = make(chan int)
		go func(s []string, i int) {
			var inter interface{}
			//fmt.Println("GOruntine!!", i)
			for _, i := range s {
				err := json.Unmarshal([]byte(i), &inter)
				handleError(err)
				err = c.Insert(inter)
				handleError(err)
			}

			chs[i] <- 1
		}(sdata[i:(i+1)*1000], i)
		//if i%6000 == 0 {
		//	time.Sleep(time.Second / 10)
		//}
	}

	//	timeout := make(chan bool)

	//go func() {
	//	time.Sleep(time.Second * 10)
	//	close(timeout)

	//}()
	for i, ch := range chs {
		<-ch
		fmt.Print(i, ",")
	}

	fmt.Println(len(sdata))
	fmt.Println("1")
	fmt.Println("2")
	fmt.Println("3")
	fmt.Println("4")
	fmt.Println("5")
	fmt.Println("6")

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(err)
		}
	}()
}

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
