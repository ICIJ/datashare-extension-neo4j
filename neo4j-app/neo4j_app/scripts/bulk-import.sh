#!/usr/bin/env bash

function _export_global_variables() {
    export ROOT_DIR
    ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
}

function _helpers() {
    function _check_jq() {
        command -v poetry 1>/dev/null 2>&1 || _exit_with_message "jq is not installed"
    }

    function _check_neo4j_home() {
        if [[ -z $NEO4J_HOME ]]; then _exit_with_message "NEO4J_HOME is not defined";fi
    }

    function _exit_with_message() {
        echo "$1"
        exit "${2:-1}"
    }

    function _parse_nodes() {
        local cmd
        cmd=
        local nodes
        nodes=$(jq -r '.nodes[] | "\(if .labels | length > 0 then .labels | join("|") + "=" else "" end)\"\(.headerPath),\(.nodePaths | join(","))\""' "$ROOT_DIR"/metadata.json)
        for n in $nodes;do
            cmd+=" --nodes=$n"
        done
        echo "$cmd"
    }

    function _parse_relationships() {
        local cmd
        cmd=
        local rels
        rels=$(
            jq -r '.relationships[] | "\(if .types | length > 0 then .types | join("|") + "=" else "" end)\"\(.headerPath),\(.relationshipPaths | join(","))\""' "$ROOT_DIR"/metadata.json
        )
        for r in $rels;do
            cmd+=" --relationships=$r"
        done
        echo "$cmd"
    }

    function _perform_sanity_checks() {
        _check_jq
        _check_neo4j_home
    }
}

function _commands() {
    function _parse_admin_import_cmd() {
        local cmd
        local database
        database="${DB:=neo4j}"
        # We skip bad relationships since doc root relationship might point to non imported document
        cmd="$NEO4J_HOME/bin/neo4j-admin import full --skip-bad-relationships --database $database$(_parse_nodes)$(_parse_relationships)"
        echo "$cmd"
    }
}


function _main() {
    FLAG_DRY_RUN=
    DB=

    function _parse_args() {
        # Unless specified we run commands for all projects
        local arg
        local arg_value
        local skip
        for i in $(seq 1 $#); do
            j=$((i + 1))
            if [[ -z "$skip" ]]; then
                arg="${!i}"
                arg_value="${!j}"
                if [[ $arg == "--dry-run" ]]; then
                    FLAG_DRY_RUN=1
                elif [[ $arg == "--database" ]]; then
                    DB=$arg_value
                fi
            else
                unset skip
            fi
        done
    }

    set -e
    _parse_args "$@"
    _export_global_variables
    _helpers
    _perform_sanity_checks
    _commands

    if [[ -n $FLAG_DRY_RUN ]];then
        _parse_admin_import_cmd
    else
        eval "$(_parse_admin_import_cmd)"
    fi
}

_main "$@"
