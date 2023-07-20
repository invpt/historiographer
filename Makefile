build: *.go src/*.go
	go build

run: build
	./drp-delta

clean:
	rm drp-delta