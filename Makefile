
main: sqlite3.o ctx.o main-cli.o chunker.o
	cc -o main-cli sqlite3.o ctx.o main-cli.o chunker.o
