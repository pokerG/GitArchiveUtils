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
	"time"
	//"labix.org/v2/mgo/bson"
)

const FILESIZE int = 20971520

func main() {
	if len(os.Args) > 1 {

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

	buf := make([]byte, FILESIZE)

	var num int = 0

	for {
		n, err := gr.Read(buf[num:])
		if err == io.EOF {
			break
		}
		num += n
		handleError(err)
	}

	buf = buf[:num]

	WriteToMongo(buf)
}

func WriteToMongo(data []byte) {
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
		c := session.DB("testBig").C("Event")
		err = c.Insert(inter)

	}

	handleError(err)

}

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
