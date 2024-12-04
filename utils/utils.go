package utils

import(
	"os"
	"log"
	"encoding/json"
	"slices"
)

type Config struct{
	N_workers int `json:"n_workers"`
	Ports []string `json:"ports"`
	Out_files []string `json:"out_files"`
}

type Input struct{
	Nums []int `json:"nums"`
}

var Workers int
var MaxValue int

func ReadConfig() Config {
	
	//retrieve configuration data from file (# of workers and worker ports)

	file, err := os.Open("config/config.json")
	defer file.Close()

	CheckError(err)

	byteContent,_ := os.ReadFile("config/config.json")

	var config Config

	json.Unmarshal(byteContent, &config)

	Workers = config.N_workers

	return config
}

func ReadInput() Input {

	//retrieve input sequence from file

	file, err := os.Open("inout_files/input.json")
	defer file.Close()

	CheckError(err)

	byteContent, _ := os.ReadFile("inout_files/input.json")

	var input Input

	json.Unmarshal(byteContent, &input)

	MaxValue = slices.Max(input.Nums)

	return input
}

func CheckError(err error) {
	
	if err != nil {
		log.Fatal(err)
	}
}

func SplitInput(nums []int, n_workers int) [][]int{
	
	if len(nums) == 0 {
		return nil
	}

	if n_workers <= 0 {
		return nil
	}

	if n_workers == 1 {
		return [][]int{nums}
	}

	numsSplit := make([][]int, n_workers)

	if n_workers > len(nums) {
		for i := 0; i<n_workers; i++ {
			numsSplit[i] = []int{nums[i]}
		}
		return numsSplit
	}

	for i:=0; i<n_workers; i++ {

		min := (i*len(nums))/n_workers
		max := ((i+1)*len(nums))/n_workers

		numsSplit[i] = nums[min:max]
	}

	return numsSplit

}