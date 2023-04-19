NEO4J_CSV_COL = "node_col"

# TODO: replicate other doc attributes
DOC_NODE = "Document"
DOC_CONTENT_LENGTH = "contentLength"
DOC_CONTENT_TYPE = "contentType"
DOC_DIRNAME = "dirname"
DOC_ID = "id"
DOC_ID_CSV = f"ID({DOC_NODE})"
DOC_EXTRACTION_DATE = "extractionDate"
DOC_PATH = "path"
DOC_ROOT_ID = "rootId"
DOC_ROOT_REL_LABEL = "HAS_PARENT"
DOC_COLUMNS = {
    DOC_ID: {NEO4J_CSV_COL: DOC_ID_CSV},
    DOC_DIRNAME: {},
    DOC_CONTENT_TYPE: {},
    DOC_CONTENT_LENGTH: {NEO4J_CSV_COL: "LONG"},
    DOC_EXTRACTION_DATE: {NEO4J_CSV_COL: "DATETIME"},
    DOC_PATH: {},
}

MIGRATION_COMPLETED = "completed"
MIGRATION_LABEL = "label"
MIGRATION_NODE = "Migration"
MIGRATION_STARTED = "started"
MIGRATION_STATUS = "status"
MIGRATION_VERSION = "version"

# TODO: replicate other named entities attributes
NE_APPEARS_IN_DOC = "APPEARS_IN"
NE_ID = "id"
NE_IDS = "mentionIds"
NE_CATEGORY = "category"
NE_DOC_ID = "documentId"
NE_EXTRACTOR = "extractor"
NE_EXTRACTORS = "mentionExtractors"
NE_EXTRACTOR_LANG = "extractorLanguage"
NE_MENTION = "mention"
NE_MENTION_NORM = "mentionNorm"
NE_MENTION_NORM_TEXT_LENGTH = "mentionNormTextLength"
NE_NODE = "NamedEntity"
NE_OFFSETS = "offsets"
NE_COLUMNS = {
    NE_ID: {},
    NE_CATEGORY: {},
    NE_DOC_ID: {},
    NE_EXTRACTOR: {},
    NE_EXTRACTOR_LANG: {},
    NE_MENTION: {},
    NE_MENTION_NORM: {},
    NE_MENTION_NORM_TEXT_LENGTH: {NEO4J_CSV_COL: "INT"},
    NE_OFFSETS: {NEO4J_CSV_COL: "LONG[]"},
}
NE_APPEARS_IN_DOC_COLS = {
    NE_EXTRACTORS: {NEO4J_CSV_COL: "STRING[]"},
    NE_EXTRACTOR_LANG: {},
    NE_IDS: {NEO4J_CSV_COL: "STRING[]"},
    NE_OFFSETS: {NEO4J_CSV_COL: "LONG[]"},
}
NEO4J_ARRAY_SPLIT_CHAR = "|"
NEO4J_CSV_END_ID = ":END_ID"
NEO4J_CSV_ID = ":ID"
NEO4J_CSV_LABEL = ":LABEL"
NEO4J_CSV_START_ID = ":START_ID"
NEO4J_CSV_TYPE = ":TYPE"
