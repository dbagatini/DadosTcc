#########
# INMET #
#########

if(!require(RSQLite)){
  install.packages("RSQLite")
  library(RSQLite)
}

library(DBI)
library(readr)


setwd("~/dados/R_INMET")

cat("\nCriando estrutura de pastas R_INMET na sua pasta de arquivos principal (home)\n\n")

if (!dir.exists("~/dados/R_INMET/dadosBrutos/arquivos")){
  dir.create("~/R_INMET/dadosBrutos/arquivos", recursive = T)  
}

cat("Não é possível fazer o download automátido dos dados pois o INMET os libera diretamente. \n")
cat("Faça o download manual e descompacte os arquivos na pasta R_INMET/dadosBrutos/arquivos\n")
cat("que foi criada em sua pasta de arquivos principal (home)\n\n")

cat("Após a extração, renomei as pastas como 'automaticas' e 'convencionais' para cada tipo de dado\n")

continuar <- readline("Esses procedimentos já foram realizados? (s N)")

if (toupper(continuar) != 'S'){
  stop()
}

if (file.exists("bdINMET.sqlite"))
  gerarBase <- readline("Deseja gerar a base de dados novamente? (s N)")


if (toupper(gerarBase) == "S"){
  if (file.exists("bdINMET.sqlite")){
    file.remove("bdINMET.sqlite")
  }
  
  nome <- character()
  codigo <- character()
  latitude <- numeric()
  longitude <- numeric()
  altitude <- numeric()
  situacao <- character()
  dataInicial <- character()
  dataFinal <- character()
  periodicidade <- character()
  tipo <- character()
  
  cat("Iniciando gravação da base de dados\n")
  
  bd <- dbConnect(SQLite(), "bdINMET.sqlite")
  
  # criando a tabela mão pois essa base é toda bichada. Parece que os caras fazem de propósito
  dbExecute(bd, "CREATE TABLE medicoes (
                  	id INTEGER,
                  	codigo TEXT,
                    Data_Medicao TEXT,
                  	EVAPORACAO_DO_PICHE_DIARIA_mm REAL,
                  	INSOLACAO_TOTAL_DIARIO_h REAL,
                  	PRECIPITACAO_TOTAL_DIARIO_mm REAL,
                  	TEMPERATURA_MAXIMA_DIARIA_C REAL,
                  	TEMPERATURA_MEDIA_COMPENSADA_DIARIA_C REAL,
                  	TEMPERATURA_MINIMA_DIARIA_C REAL,
                  	UMIDADE_RELATIVA_DO_AR_MEDIA_DIARIA REAL,
                  	UMIDADE_RELATIVA_DO_AR_MINIMA_DIARIA REAL,
                  	VENTO_VELOCIDADE_MEDIA_DIARIA_m_s REAL,
                  	PRESSAO_ATMOSFERICA_MEDIA_DIARIA_mB REAL,
                  	TEMPERATURA_DO_PONTO_DE_ORVALHO_MEDIA_DIARIA_C REAL,
                  	TEMPERATURA_MEDIA_DIARIA_C REAL,
                  	VENTO_RAJADA_MAXIMA_DIARIA_m_s REAL,
                  	X TEXT
                  )")
  
  
  id <- 1
  
  tryCatch(
    {
      arquivos <- list.files(path="./dadosBrutos/arquivos/convencionais", pattern="*.csv", full.names=TRUE, recursive=FALSE)
      lapply(arquivos, function(a) {
        nomeArquivo <- strsplit(tail(strsplit(a, "/")[[1]], n=1), "[.]")[[1]][1]
        
        cat(nomeArquivo, "\tLendo")
        
        dados <- read_lines(a, n_max=10)
        
        cod <- strsplit(dados[2], ":")[[1]][2]
        
        nome <<- c(nome, strsplit(dados[1], ":")[[1]][2])
        codigo <<- c(codigo, cod)
        latitude <<- c(latitude, strsplit(dados[3], ":")[[1]][2])
        longitude <<- c(longitude, strsplit(dados[4], ":")[[1]][2])
        altitude <<- c(altitude, strsplit(dados[5], ":")[[1]][2])
        situacao <<- c(situacao, strsplit(dados[6], ":")[[1]][2])
        dataInicial <<- c(dataInicial, strsplit(dados[7], ":")[[1]][2])
        dataFinal <<- c(dataFinal, strsplit(dados[8], ":")[[1]][2])
        periodicidade <<- c(periodicidade, strsplit(dados[9], ":")[[1]][2])
        tipo <<- c(tipo, "convencional")
        
        if (cod == "83084"){
          cat(nomeArquivo, "\tIgnorado pois está com dados bichados\n")
          id <<- id+1
          return()
        }
        
        t <- read.csv(a, header=TRUE, skip = 10, dec = ".", sep = ";", stringsAsFactors = F, quote="", fileEncoding="UTF-8")
        
        colnames(t) <- gsub("_$", "", gsub("\\.", "_", gsub("[\\.]{2,}", ".", colnames(t))))
        
        t["id"] <- rep(id, nrow(t))
        t["codigo"] <- rep(cod, nrow(t))
        
        t[t=="null"] <- NA
        
        cat("\t Gravando")
        dbWriteTable(bd, "medicoes", t, append=T)
        
        cat("\tOK\n")
        
        id <<- id+1
      })
      
      arquivos <- list.files(path="./dadosBrutos/arquivos/automaticas", pattern="*.csv", full.names=TRUE, recursive=FALSE)
      lapply(arquivos, function(a) {
        nomeArquivo <- strsplit(tail(strsplit(a, "/")[[1]], n=1), "[.]")[[1]][1]
        
        cat(nomeArquivo, "\tLendo")
        
        dados <- read_lines(a, n_max=10)
        
        cod <- strsplit(dados[2], ":")[[1]][2]
        
        nome <<- c(nome, strsplit(dados[1], ":")[[1]][2])
        codigo <<- c(codigo, cod)
        latitude <<- c(latitude, strsplit(dados[3], ":")[[1]][2])
        longitude <<- c(longitude, strsplit(dados[4], ":")[[1]][2])
        altitude <<- c(altitude, strsplit(dados[5], ":")[[1]][2])
        situacao <<- c(situacao, strsplit(dados[6], ":")[[1]][2])
        dataInicial <<- c(dataInicial, strsplit(dados[7], ":")[[1]][2])
        dataFinal <<- c(dataFinal, strsplit(dados[8], ":")[[1]][2])
        periodicidade <<- c(periodicidade, strsplit(dados[9], ":")[[1]][2])
        tipo <<- c(tipo, "automatica")
        
        t <- read.csv(a, header=TRUE, skip = 10, dec = ".", sep = ";", stringsAsFactors = F, quote="", fileEncoding="UTF-8")
        
        colnames(t) <- gsub("\\.AUT\\.\\.", "", colnames(t))
        colnames(t) <- gsub("_$", "", gsub("\\.", "_", gsub("[\\.]{2,}", ".", colnames(t))))
        
        t["id"] <- rep(id, nrow(t))
        t["codigo"] <- rep(cod, nrow(t))
        
        t[t=="null"] <- NA
        
        cat("\t Gravando")
        dbWriteTable(bd, "medicoes", t, append=T)
        
        cat("\tOK\n")
        
        id <<- id+1
      })
      
      dadosEstacoes <<- data.frame(id = 1:length(nome),
                                  nome,
                                  codigo,
                                  latitude,
                                  longitude,
                                  altitude,
                                  situacao,
                                  dataInicial,
                                  dataFinal,
                                  periodicidade,
                                  tipo)
    },
    error = function(e){
      dbDisconnect(bd)
      message(e)
    },
    finally = {
      # limpa as variáveis para liberar memória
      rm(nome)
      rm(codigo)
      rm(latitude)
      rm(longitude)
      rm(altitude)
      rm(situacao)
      rm(dataInicial)
      rm(dataFinal)
      rm(periodicidade)
      rm(tipo)
      rm(arquivos)
      rm(id)
      rm(continuar)
      rm(gerarBase)
    })
      
  #bd <- dbConnect(SQLite(), "bdINMETsqlite")
  dbWriteTable(bd, "estacoes", dadosEstacoes, overwrite=T)
  dbDisconnect(bd)
}

