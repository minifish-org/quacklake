.PHONY: doctor up up-build down status seed query smoke validate browser logs prod-up prod-down

doctor:
	./scripts/ducklake doctor

up:
	./scripts/ducklake up

up-build:
	./scripts/ducklake up --build

down:
	./scripts/ducklake down

status:
	./scripts/ducklake status

seed:
	./scripts/ducklake seed

query:
	./scripts/ducklake query

smoke:
	./scripts/ducklake smoke

validate:
	./scripts/ducklake validate

browser:
	./scripts/ducklake browser

logs:
	./scripts/ducklake logs api

prod-up:
	./scripts/ducklake prod-up

prod-down:
	./scripts/ducklake prod-down
