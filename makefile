all: README.md

README.md:
	echo '# Unix Workbench project guessinggame.sh' > README.md
	echo '* The makefile was run at: $(shell date +%Y-%m-%d:%H:%M:%S)' >> README.md
	echo '* There were $(shell wc -l < guessinggame.sh) lines in guessinggame.sh' >> README.md

clean:
	rm README.md
