package main

import (
	"log"
	"net"
	"strconv"
	"strings"
)

type IPRange struct {
	start net.IP;
	end net.IP;
}

func contains(rng IPRange, targetIP net.IP) bool {
	for i := range targetIP {
		if targetIP[i] < rng.start[i] {
			return false
		}
		if targetIP[i] > rng.end[i] {
			return false
		}
	}
	return true
}

func find(ranges []IPRange, targetIp net.IP) bool {
	if targetIp == nil {
		return false
	}
	
	for _, rng := range ranges {
		if contains(rng, targetIp) {
			return true
		}
	}
	return false;
}

func parseRange(listStr string) []IPRange {
	var res []IPRange
	var intVal int
	var err error

	elementTokens := strings.Split(listStr, ", ")
	log.Printf("%+v", elementTokens)

	for i := range elementTokens {
		byteTokens := strings.Split(elementTokens[i], ".");

		if len(byteTokens) != 4 {
			log.Printf("Error at %s", elementTokens[i])
			return nil
		}

		beginAddr := make([]byte, 4);
		endAddr := make([]byte, 4);

		for j := range byteTokens {
			if byteTokens[j] == "*" {
				beginAddr[j] = 0;
				endAddr[j] = 255;
			} else {
				addrTokens := strings.Split(byteTokens[j], "-")
				if len(addrTokens) == 1 {
					intVal, err = strconv.Atoi(addrTokens[0])
					if err != nil {
						log.Printf("Error at %s", elementTokens[i])
						return nil
					}
					beginAddr[j] = byte(intVal)
					endAddr[j] = byte(intVal)
				} else if len(addrTokens) == 2 {
					intVal, err = strconv.Atoi(addrTokens[0])
					if err != nil {
						log.Printf("Error at %s", elementTokens[i])
						return nil
					}
					beginAddr[j] = byte(intVal)
					intVal, err = strconv.Atoi(addrTokens[1])
					if err != nil {
						log.Printf("Error at %s", elementTokens[i])
						return nil
					}
					endAddr[j] = byte(intVal)
				} else {
					log.Printf("Error at %s", elementTokens[i])
					return nil
				}
			}
		}

		res = append(res, IPRange{
			net.IPv4(beginAddr[0], beginAddr[1], beginAddr[2], beginAddr[3]),
			net.IPv4(endAddr[0], endAddr[1], endAddr[2], endAddr[3]),
		})
	}

	return res;
}

