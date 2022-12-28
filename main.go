package main

import (
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

func main() {
	//redis地址
	redisHost := flag.String("host", "", "the host of the redis server")
	//redis密码
	password := flag.String("password", "", "the password of the redis server")
	//post
	port := flag.Int64("port", 6379, "the port of the redis server")
	//size
	sizeLimit := flag.Int64("size", 10, "the size what you want to find")
	flag.Parse()
	//参数判定
	if *sizeLimit < 0 || len(strings.Trim(*redisHost, " ")) == 0 {
		flag.Usage()
		return
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%v:%v", *redisHost, *port),
		Password: *password,
		DB:       0,
	})

	if err := rdb.Ping().Err(); err != nil {
		fmt.Println(err.Error())
		return
	}

	s := rdb.Info("Keyspace").Val()

	var s3 []int
	re := regexp.MustCompile(`db([\d]+)`)
	matched := re.FindAllStringSubmatch(s, -1)
	for _, match := range matched {
		i, _ := strconv.Atoi(match[1])
		s3 = append(s3, i)
	}

	var wg sync.WaitGroup

	for _, i := range s3 {
		wg.Add(1)
		go func(ii int) {
			defer func() {
				wg.Done()
			}()
			//切换数据
			rdb.Do("select", ii)
			//查询keys
			s7 := rdb.Keys("*").Val()
			//遍历key
			for _, v2 := range s7 {
				size := rdb.Do("memory", "usage", v2).Val().(int64)
				if dstSize := size / 1024 / 1024; dstSize > *sizeLimit {
					fmt.Printf("DB=%v | Key=%v | Size=%vM\n", ii, v2, dstSize)
				}
			}
		}(i)
	}
	wg.Wait()
}
