import tomlkit
import xml.etree.ElementTree as ET
from pathlib import Path

_ROOT_DIR = Path(__file__).parents[1]
_POM_XML_PATH = _ROOT_DIR / "pom.xml"
_PYPROJECT_TOML_PATH = _ROOT_DIR.joinpath("neo4j-app", "pyproject.toml")
_VERSION_PATH = _ROOT_DIR / "version"

_POM_XML_NAMESPACES = {"POM": "http://maven.apache.org/POM/4.0.0"}


def _read_version() -> str:
    with _VERSION_PATH.open() as f:
        new_version = next(f).strip()
    return new_version


def _update_pom_xml():
    ET.register_namespace("", "http://maven.apache.org/POM/4.0.0")
    tree = ET.parse(str(_POM_XML_PATH.absolute()))
    root = tree.getroot()
    version = root.find("POM:version", _POM_XML_NAMESPACES)
    new_version = _read_version()
    version.text = new_version
    tree.write(str(_POM_XML_PATH.absolute()), encoding="UTF-8", xml_declaration=True)


def _update_pyproject_toml():
    pyproject_toml = tomlkit.parse(_PYPROJECT_TOML_PATH.read_text())
    new_version = _read_version()
    pyproject_toml["tool"]["poetry"]["version"] = new_version

    _PYPROJECT_TOML_PATH.write_text(tomlkit.dumps(pyproject_toml))


if __name__ == "__main__":
    _update_pom_xml()
    _update_pyproject_toml()
