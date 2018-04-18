package wfi

import (
	"fmt"
	"io"
	"log"
	"os/exec"
)

// Up spins up docker-compose using `up` command
func Up(execLoc, fileName string, options ...string) error {
	params := make([]string, 0)
	if len(fileName) > 0 {
		params = append(params, "-f", fileName)
	}
	params = append(params, "up", "-d")
	params = append(params, options...)
	cmd := exec.Command("docker-compose", params...)
	if len(execLoc) > 0 {
		cmd.Dir = execLoc
	}
	if msg, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("cannot spin up docker-compose %s \n %v", msg, err)
	}
	return nil
}

// UpWithLogs spins up docker-compose using `up` command but not in detach mode
// One should never pass in "-d" as option because it will display logs
func UpWithLogs(execLoc, fileName string, options ...string) (io.ReadCloser, error) {
	params := make([]string, 0)
	if len(fileName) > 0 {
		params = append(params, "-f", fileName)
	}
	params = append(params, "up")
	params = append(params, options...)
	cmd := exec.Command("docker-compose", params...)
	if len(execLoc) > 0 {
		cmd.Dir = execLoc
	}
	rc, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("cannot spin up docker-compose %v", err)
	}
	return rc, nil
}

// Down cleans up services with docker-compose using `down` command
func Down(execLoc, fileName string) {
	params := make([]string, 0)
	if len(fileName) > 0 {
		params = append(params, "-f", fileName)
	}
	params = append(params, "down")
	cmd := exec.Command("docker-compose", params...)
	if len(execLoc) > 0 {
		cmd.Dir = execLoc
	}
	if msg, err := cmd.CombinedOutput(); err != nil {
		// Just exit here. In most cases, we'll have to fix the containers manually
		log.Fatal(fmt.Errorf("cannot clean up with docker-compose %s \n %v", msg, err))
	}
	log.Printf("Succesfully cleaned up docker-compose %v", fileName)
}
