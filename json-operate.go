package main

import (
	"fmt"
	"os"
	"encoding/json"
)

type EncodeOpeate interface{
	Encode() ([]byte,error)
	Decode([]byte) error 
	Get(key string) interface{}
	Set(key string, v interface{})
	GetbyIndex(index int) interface{}
	Map()(map[string]interface{},error)
	Array()([]interface{}.error)
	
	Bool()(bool,error)
	Float64()(float64,error)
	Int()(int,error)
	Int64()(int,error)
	String()(string,error)
	Bytes()([]byte,error)

}

type Json{
	data interface{}
}

