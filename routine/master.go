package routine

import "github.com/PatchouliG/lsm-db/metadata"

func initDB() {

}

func CoreRoutine(workDir string) {
	// todo
	// build metadata if load a exits db
	//metadata := loadMetadata()
	// create db reader ,db write routine
	createWorker()
	handleHttp()
}

func loadMetadata() *metadata.Metadata {
	panic("")
}

// todo return
func createWorker() {

}

func handleHttp() {
	for {
		//	handle http readRequest
		//	dispatch readRequest

	}
}
