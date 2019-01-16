import configparser


def read_table(name):
    config = configparser.ConfigParser()
    config.read("./conf/cubes.ini", encoding="utf-8")
    cubes = config["cubes"].get("names").split(" ")
    if name in cubes:
        cube = config[name]
        if cube.get("type") == "csv":
            data_cube = {name: {"path": cube.get("path"),
                                "type": cube.get("type"),
                                "sep": cube.get("sep"),
                                "header": cube.getboolean("header")}}
        else:
            data_cube = {name: {"path": cube.get("path"),
                                "type": cube.get("type")}}
        return data_cube[name]
    else:
        raise NameError("no such view " + name)


def tables():
    config = configparser.ConfigParser()
    config.read("./conf/cubes.ini", encoding="utf-8")
    cubes = config["cubes"].get("names").split(" ")
    return cubes

