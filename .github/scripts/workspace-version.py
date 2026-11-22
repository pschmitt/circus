import tomllib

with open("Cargo.toml", "rb") as cargo:
    print(tomllib.load(cargo)["workspace"]["package"]["version"])
