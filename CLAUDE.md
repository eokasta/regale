# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status do projeto

Este repositório está em estágio inicial de planejamento. Ainda não há código — apenas este arquivo, o README e a LICENSE. O conteúdo abaixo descreve o design pretendido, definido pelo dono do projeto, para guiar a primeira implementação.

## O que é o Regale

Regale ("prateleira" em alemão) é um framework Python de ETL modular, pensado para ser usado como base por outros projetos de ETL (não é uma aplicação final em si).

Objetivos centrais:

- **Modular por módulos com prioridades configuráveis** — o framework é composto por módulos independentes que podem ser priorizados/configurados pelo usuário do framework.
- **Escalável vertical e horizontalmente** — deve suportar tanto o aumento de recursos em uma única máquina quanto a distribuição de processamento entre múltiplos workers/máquinas.
- **Integração nativa obrigatória com `pandas`** desde a primeira versão. Essa é uma decisão de arquitetura fixa, não opcional.
- **Fontes de dados diversas, com foco inicial em SQL** — a extração deve ser genérica o suficiente para múltiplas fontes, mas o caso de uso inicial e prioritário é extração via queries SQL.

## Arquitetura de workers (pipeline ETL)

O processamento é dividido em três papéis de worker, desacoplados entre si:

1. **Query workers** — responsáveis pela extração (execução de queries/leitura de fontes).
2. **Transform workers** — responsáveis pela transformação dos dados extraídos (via `pandas`).
3. **Load workers** — responsáveis por carregar os dados transformados no destino.

Requisito chave: deve ser possível **dispersar o processamento** entre esses workers de forma independente — por exemplo, alocar workers mais fortes (mais CPU/memória) para a etapa de transformação e workers mais fracos para query/load, ou qualquer outra combinação. Essa alocação é uma decisão do usuário do framework, não fixa pelo framework.

## Filosofia de API

A interação com o framework deve ser **simples por padrão, mas robusta e escalável em configuração**:

- Para ETLs simples: pouco código deve ser suficiente para configurar um pipeline funcional (extração → transformação → carga).
- Para ETLs complexos: a mesma API deve permitir configuração avançada (prioridades de módulo, alocação de workers, escalabilidade horizontal) sem exigir reescrever o pipeline básico.

Esse equilíbrio entre simplicidade no caso comum e profundidade de configuração no caso avançado é um requisito de design central, não um detalhe de implementação.

## Convenção de idioma

Documentação e comunicação em português. Nomenclatura de código (nomes de módulos, classes, funções, variáveis, mensagens de commit no estilo convencional) sempre em inglês.
