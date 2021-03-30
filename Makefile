build:
	@protoc --go_out . protos/*.proto
	go build -o ./dataapp main.go
