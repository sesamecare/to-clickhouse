.PHONY: all clean

# Postgres database things
export PGUSER ?= postgres
export PGPASSWORD ?= postgres
export PGHOST ?= localhost

src_files := $(shell find src -name '*.ts')
build_files := $(patsubst src/%.ts,$(build_dir)/%.js,$(src_files))

all: dbi ts

# Typescript items
ts: $(word 1, $(build_files))

$(word 1, $(build_files)): $(src_files)
	./node_modules/.bin/tsc -p tsconfig.build.json

dbi: __tests__/generated/database.ts

clean:
	rm -rf __tests__/generated
	rm -rf build/

__tests__/generated/database.ts:
	echo "Generating database types"
	yarn ts-node __tests__/db.fixtures.ts
	DATABASE_URL=postgres://$(PGUSER):$(PGPASSWORD)@$(PGHOST)/chtest yarn kysely-codegen \
	        --dialect postgres --schema public \
	        --out-file __tests__/generated/database.ts
	yarn lint --fix __tests__/generated/database.ts