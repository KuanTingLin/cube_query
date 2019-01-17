# -*- coding: utf-8 -*-
from model.model import get_cubes


def read_table(name):
    config = get_cubes()
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
    config = get_cubes()
    cubes = config["cubes"].get("names").split(" ")
    return cubes
