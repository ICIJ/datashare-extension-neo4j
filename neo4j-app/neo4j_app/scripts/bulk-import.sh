#!/usr/bin/env bash

function _export_global_variables() {
    export ROOT_DIR
    ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
}

function _helpers() {
    function _check_jq() {
        command -v jq 1>/dev/null 2>&1 || _exit_with_message "jq is not installed"
    }

    function _check_neo4j_home() {
        if [[ -z $NEO4J_HOME ]]; then _exit_with_message "NEO4J_HOME is not defined";fi
    }

    function _exit_with_message() {
        echo "$1"
        exit "${2:-1}"
    }

    function _parse_nodes() {
        local parsed
        parsed=
        local nodes
        nodes=$(jq -r '.nodes[] | "\(if .labels | length > 0 then .labels | join("|") + "=" else "" end)\"\(.headerPath),\(.nodePaths | join(","))\""' "$ROOT_DIR"/metadata.json)
        for n in $nodes;do
            parsed+=" --nodes=$n"
        done
        echo "$parsed"
    }

    function _parse_relationships() {
        local parsed
        parsed=
        local rels
        rels=$(
            jq -r '.relationships[] | "\(if .types | length > 0 then .types | join("|") + "=" else "" end)\"\(.headerPath),\(.relationshipPaths | join(","))\""' "$ROOT_DIR"/metadata.json
        )
        for r in $rels;do
            parsed+=" --relationships=$r"
        done
        echo "$parsed"
    }

    function _parse_db() {
        local parsed
        parsed="$(jq -r '.db' "$ROOT_DIR"/metadata.json)"
        echo "$parsed"
    }

    function _perform_sanity_checks() {
        _check_jq
        _check_neo4j_home
    }
}

function _commands() {
    function _parse_admin_import_cmd() {
        local parsed
        # We skip bad relationships since doc root relationship might point to non imported document
        parsed="$NEO4J_HOME/bin/neo4j-admin database import full --array-delimiter=\"|\" --skip-bad-relationships$(_parse_nodes)$(_parse_relationships) $(_parse_db)"
        echo "$parsed"
    }
}


function _main() {
    FLAG_DRY_RUN=

    function _parse_args() {
        # Unless specified we run commands for all projects
        local arg
        local skip
        for i in $(seq 1 $#); do
            j=$((i + 1))
            if [[ -z "$skip" ]]; then
                arg="${!i}"
                if [[ $arg == "--dry-run" ]]; then
                    FLAG_DRY_RUN=1
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
