package main

import (
	"encoding/csv"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path"
)

/*
Enumerates two levels of directories, starting at the root directory.
Stores results in "dirs.csv"
*/

func main() {
	log.Info("Start")

	if len(os.Args) != 2 {
		log.Fatal("Usage: ./enumerate-directories <rootdir>")
	}

	f, err := os.Create("dirs.csv")
	defer f.Close()
	writer := csv.NewWriter(f)

	rootDir := os.Args[1]
	dirs, err := ioutil.ReadDir(rootDir)
	if err != nil {
		log.Fatal(err)
	}

	for i, dir := range dirs {
		if i%1000 == 0 {
			log.Infof("Processed %d directories", i)
		}

		subdirs, err := ioutil.ReadDir(path.Join(rootDir, dir.Name()))
		if err != nil {
			log.Error("Error for ", dir.Name(), " : ", err)
		}

		for _, sd := range subdirs {
			err = writer.Write([]string{dir.Name(), sd.Name()})
			if err != nil {
				log.Error(err)
			}
			writer.Flush()
		}

	}

	log.Info("End")
}
