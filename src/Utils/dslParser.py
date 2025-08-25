import re
from pathlib import Path


TAGRE = re.compile(r"<(\w+)(?::\s*@(\w+))?(<.*?)>") # <tag: @Class attr=value>
ATTRRE = re.compile(r"(\w+)=((\".*?\")|(\'.*?\')|(\S+))") # attr=value or attr="value" or attr='value'

class DSLParser:
    @staticmethod
    def parseFile(path: Path):
        result = {}
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("//"):
                    continue
                parsed = DSLParser.parseLine(line)
                if parsed:
                    result[parsed["tag"]] = parsed
        return result
    
    @staticmethod
    def parseLine(line: str):
        match = TAGRE.match(line)
        if not match:
            return None
        tag, clsName, attrString = match.groups()
        attrs = {}
        for a in ATTRRE.finditer(attrString):
            key, value = a.groups()
            if value.startswith("[") and value.endswith("]"):
                value = eval(value)
            attrs[key] = value
        return {"tag": tag, "class": clsName, "attributes": attrs}