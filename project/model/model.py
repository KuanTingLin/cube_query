# -*- coding: utf-8 -*-
import configparser


def get_cubes():
    config = configparser.ConfigParser()
    config.read("./conf/cubes.ini", encoding="utf-8")
    return config
