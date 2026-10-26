# Dicionário de dados

## Silver · `silver_geracao_distribuida`

Uma linha por empreendimento (versão mais recente).

| Coluna | Tipo | Origem (CSV ANEEL) | Regra |
|---|---|---|---|
| `cod_empreendimento` | string | `CodEmpreendimento` | chave; obrigatória |
| `data_conjunto_dados` | date | `DatGeracaoConjuntoDados` | usada no desempate da deduplicação |
| `data_conexao` | date | `DthAtualizaCadastralEmpreend` | obrigatória e não futura |
| `cnpj_distribuidora` | string | `NumCNPJDistribuidora` | apenas dígitos |
| `sigla_distribuidora` | string | `SigAgente` | maiúsculas |
| `nome_distribuidora` | string | `NomAgente` | |
| `classe_consumo` | string | `DscClasseConsumo` | |
| `subgrupo_tarifario` | string | `DscSubGrupoTarifario` | |
| `uf` | string | `SigUF` | 2 letras |
| `regiao` | string | `NomRegiao` | |
| `cod_municipio_ibge` | string | `CodMunicipioIbge` | |
| `municipio` | string | `NomMunicipio` | *Initcap* |
| `tipo_consumidor` | string | `SigTipoConsumidor` | PF / PJ |
| `modalidade` | string | `DscModalidadeHabilitado` | |
| `qtd_uc_recebe_credito` | int | `QtdUCRecebeCredito` | |
| `tipo_geracao` | string | `SigTipoGeracao` | UFV, UTE, EOL, CGH |
| `fonte_geracao` | string | `DscFonteGeracao` | |
| `porte` | string | `DscPorte` | micro (≤ 75 kW) / mini |
| `latitude`, `longitude` | double | `NumCoordN/E…` | decimal brasileiro; dentro do território |
| `potencia_instalada_kw` | double | `MdaPotenciaInstaladaKW` | decimal brasileiro; > 0 |
| `_id_execucao`, `_ingerido_em` | string, timestamp | — | linhagem |

**Descartadas por LGPD:** `NumCPFCNPJ`, `NomeTitularEmpreendimento`.

## Quarentena · `silver_geracao_distribuida_quarentena`

Mesmas colunas da silver, mais `_motivos_rejeicao` (array) com uma ou mais regras:
`chave_presente`, `data_conexao_valida`, `potencia_positiva`, `uf_valida`, `coordenadas_no_brasil`.

## Gold

| Tabela | Grão | Chave |
|---|---|---|
| `gold_fato_geracao_distribuida` | empreendimento | `cod_empreendimento` |
| `gold_dim_localidade` | município | `sk_localidade` (xxhash64) |
| `gold_dim_fonte` | tipo × fonte × porte | `sk_fonte` |
| `gold_dim_classe_consumo` | classe × subgrupo × tipo × modalidade | `sk_classe` |
| `gold_dim_distribuidora` | distribuidora | `sk_distribuidora` |
| `gold_dim_calendario` | dia (2008 a 2030) | `sk_data` (yyyyMMdd) |
| `gold_agg_potencia_uf_mes` | UF × mês | `uf`, `ano_mes` |
| `log_qualidade` | checagem × execução | `id_execucao`, `checagem`, `tabela` |
