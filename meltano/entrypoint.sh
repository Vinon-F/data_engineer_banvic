#!/bin/sh
set -eu

# BANVIC_DATA_DIR aponta pra uma pasta MONTADA em runtime (não copiada na
# imagem) contendo o .zip de origem. Ajustável na orquestração do pipeline
# via env var, sem precisar rebuildar a imagem.
zip_file=$(find "$BANVIC_DATA_DIR" -maxdepth 1 -iname '*.zip' | head -n 1)

if [ -z "$zip_file" ]; then
    echo "Nenhum .zip encontrado em BANVIC_DATA_DIR=$BANVIC_DATA_DIR" >&2
    exit 1
fi

mkdir -p data/csvs
# -j (junk-paths) achata qualquer subpasta dentro do zip, já que
# files_def.json espera data/csvs/<entidade>.csv direto, sem subpasta.
unzip -j -o "$zip_file" -d data/csvs

exec meltano "$@"
