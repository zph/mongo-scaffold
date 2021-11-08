.PHONY: clean start setup stop seed

clean:
	@mlaunch stop
	@rm -rf ./data

start:
	@mlaunch start

stop:
	@mlaunch stop

setup:
	@./bin/setup

seed:
	@./bin/seed

connect:
	@mongo keyhole
