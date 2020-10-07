package main

import "github.com/PatchouliG/wisckey-db/routine"

func Init() {
	//	todo set log config
	//	set work dir
}
func main() {
	Init()
	// read from command line
	var workDir string
	routine.CoreRoutine(workDir)

}
