.PHONY:
	clean
	all
	test
	list

list:
	$(MAKE) -C sm2a list

all:
	$(MAKE) -C sm2a all

sm2a-local-run:
	$(MAKE) -C sm2a sm2a-local-run

sm2a-local-init:
	$(MAKE) -C sm2a sm2a-local-init

sm2a-local-stop:
	$(MAKE) -C sm2a sm2a-local-stop

sm2a-deploy:
	$(MAKE) -C sm2a sm2a-deploy

sm2a-local-build:
	$(MAKE) -C sm2a sm2a-local-build

clean:
	$(MAKE) -C sm2a clean

test:
	pytest tests
