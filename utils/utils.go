package utils

import(
	"os"
	"log"
	"encoding/json"
	"math"
	"fmt"
)

type Config struct{
	Ports []string `json:"ports"`
	Master string `json:"master"`
	Map_nodes []int `json:map_nodes`
	Reduce_nodes []int `json:reduce_nodes`
	Out_files []string `json:"out_files"`
}

type Input struct{
	Nums []int `json:"nums"`
}

type SampledInput struct{
	SampledNums []int `json:"sampledNums"`
}

var Workers int
var Configuration Config

func ReadConfig() Config {
	
	//retrieve all configuration data from file config.json

	file, err := os.Open("config/config.json")
	defer file.Close()

	CheckError(err)

	byteContent,_ := os.ReadFile("config/config.json")

	json.Unmarshal(byteContent, &Configuration)

	Workers = len(Configuration.Ports)

	return Configuration
}

func ReadInput() Input {

	//retrieve input sequence from file

	file, err := os.Open("inout_files/input.json")
	defer file.Close()

	CheckError(err)

	byteContent, _ := os.ReadFile("inout_files/input.json")

	var input Input

	json.Unmarshal(byteContent, &input)

	return input
}

func CheckError(err error) {
	
	if err != nil {
		log.Fatal(err)
	}
}

func SplitInput(nums []int, n_workers int) [][]int{
	
	// function to partition the input data in n balanced chunks, where n = # of map workers

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
		for i := 0; i<len(nums); i++ {
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

func SampleInput(nums []int) {

	var sampledInput SampledInput
	
	// USO LA TECNICA DEL CAMPIONAMENTO ISTOGRAFICO CON ADATTAMENTO DINAMICO

	// trovo minimo e massimo

	min, max := math.MaxInt, math.MinInt
	for _, val := range nums{
		if val<min{
			min=val
		}
		if val>max{
			max=val
		}
	}

	// calcolo l'istogramma

	nBins := 10

	counts := make([]int, nBins)
	binWidth:=float64(max-min)/float64(nBins)

	for _, val := range nums{
		binIndex:=int(float64(val-min)/binWidth)
		if binIndex>=nBins{
			binIndex = nBins-1
		}
		counts[binIndex]++
	}

	fmt.Println("Counts: ", counts)

	// calcolo i range per ciascun nodo di Reduce

	ranges := make([]int, len(Configuration.Reduce_nodes)-1)
	bucketDim := len(nums)/len(Configuration.Reduce_nodes)

	currentBin := 0
	count:=0
	for i:=0;i<len(Configuration.Reduce_nodes)-1;i++ {
		for count<bucketDim && currentBin<nBins{
			count+=counts[currentBin]
			currentBin++
		}
		ranges[i]=min+int(binWidth*float64(currentBin))
		count=0
	}

	sampledInput.SampledNums = ranges

	// writing SampledInput sequence to file
	_, err := os.OpenFile("utils/sampled.json", os.O_CREATE | os.O_TRUNC | os.O_WRONLY, 0644)
	CheckError(err)

	sampled, _ := json.Marshal(sampledInput)
    err = os.WriteFile("utils/sampled.json", sampled, 0644)
}

func GetSampledInput() SampledInput{
	var sampledInput SampledInput

	file, err := os.Open("utils/sampled.json")
	defer file.Close()

	CheckError(err)

	byteContent, _ := os.ReadFile("utils/sampled.json")

	json.Unmarshal(byteContent, &sampledInput)

	return sampledInput
}