all: bin

bin:
	python bootstrap.py
	cp buildout.cfg.example buildout.cfg
	bin/buildout -N

upd_src: bin
	bin/develop st
	bin/develop up

pin_sandbox: bin
	bin/python pinner.py
	git show

push_to_ea2:
	git push origin ea2_master

upd_pin:
	make upd_src
	make pin_sandbox

upd_pin_push:
	make upd_src
	make pin_sandbox
	make push_to_ea2
