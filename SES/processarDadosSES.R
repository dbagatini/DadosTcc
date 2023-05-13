#########
#  SES  #
#########

if(!require(RSQLite)){
  install.packages("RSQLite")
  library(RSQLite)
}

library(DBI)
#library(readr)

cat("\nCriando estrutura de pastas R_SES na sua pasta de arquivos principal (home\n\n")

if (!dir.exists("~/dados/R_SES/dadosBrutos/arquivos")){
  dir.create("~/dados/R_SES/dadosBrutos/arquivos", recursive = T)  
}


setwd("~/dados/R_SES")

if (!file.exists("./dadosBrutos/BaseCompleta.zip")){
  cat("\nRealizando o download da base de dados a partir do site da SES. ")
  cat("Essa operação pode demorar, dependendo de sua conexão.\n\n")
  cat("Caso o download esteja muito lento, a operação será cancelada após 5 minutos, ")
  cat("devendo então ser feita manualmente (salvar arquivo na pasta R_SES/dadosBrutos/\n\n")
  
  
  
  tryCatch(
    {
      timeoutOriginal <- getOption("timeout")
      options(timeout=3000)
      
      download.file(urlDadosSES, "./dadosBrutos/BaseCompleta.zip")
      cat("Download finalizado\n\n")
    },
    error = function(e) {
      cat("Erro ao realizar download, obtenha o arquivo manualmente e salve-o em ")
      cat("R_SES/dadosBrutos/ com o nome de BaseCompleta.zip.\n\n")
      file.remove("./dadosBrutos/BaseCompleta.zip")
      },
    finally = {
      options(timeout=timeoutOriginal)
      print("ok")
    })  
  
} else {
  cat("Arquivo de dados encontrado, nenhum download será realizado\n\n")
}

if (length(list.files(path="./dadosBrutos/arquivos/", pattern="*.csv", full.names=TRUE, recursive=FALSE)) > 0){
  desconpactarDados <- readline("Deseja descompactar o arquivo e substituir os arquivos antigos por versões recentes? (s N)")
}

if (toupper(desconpactarDados) == "S"){
  cat("Desconpactando arquivo (os arquivos anteriores serão substituídos")
  unzip("./dadosBrutos/BaseCompleta.zip", exdir="./dadosBrutos/arquivos/")
}


if (file.exists("bdSES.sqlite"))
  gerarBase <- readline("Deseja gerar a base de dados novamente? (s N)")


if (toupper(gerarBase) == "S"){
  bd <- dbConnect(SQLite(), "bdSES.sqlite")
  
  cat("Iniciando gravação da base de dados\n")
  arquivos <- list.files(path="./dadosBrutos/arquivos/", pattern="*.csv", full.names=TRUE, recursive=FALSE)
  lapply(arquivos, function(a) {
    nomeTabela <- strsplit(tail(strsplit(a, "/")[[1]], n=1), "[.]")[[1]][1]
    
    fixarLargura <- paste0(rep(" ", 30-nchar(nomeTabela)), collapse = "")
    
    cat(nomeTabela, fixarLargura, " Lendo dados")
    tryCatch({
      t <- read.csv(a, header=TRUE, dec = ",", sep = ";", stringsAsFactors = F, quote="", fileEncoding="windows-1252")
    },
    warning = function(e){
      cat("\tProblemas na leitura")
    })
    
    cat("\tGravando ")
    dbWriteTable(bd, nomeTabela, t, overwrite=T)
    
    cat("\tGravação completa\n")
    
  })
  
  dbDisconnect(bd)
}

query <- "SELECT 
            sc.Noenti
            , su.UF 
            , su.damesano
            , SUM(su.premio_dir) as premio_dir 
            , sum(su.sin_dir) as sin_dir
            , sum(su.sin_dir)/SUM(su.premio_dir) as razao
          from SES_UF2 su
          left join Ses_cias sc on su.coenti = sc.Coenti 
          where su.ramos in (1161, 1130, 1101, 1102)
          GROUP BY su.coenti, su.UF, su.damesano 
          order by su.coenti, su.UF, su.damesano "

bd <- dbConnect(SQLite(), "bdSES.sqlite")
dados_Uf_Cia <- dbGetQuery(bd, query)
dbDisconnect(bd)

dados_Uf_Cia[[3]] <- paste0(substring(dados_Uf_Cia[[3]],5), "/", substring(dados_Uf_Cia[[3]],0, 4), sep="")

query <- "SELECT 
            su.UF 
            , su.damesano
            , SUM(su.premio_dir) as premio_dir 
            , sum(su.sin_dir) as sin_dir
            , sum(su.sin_dir)/SUM(su.premio_dir) as razao
          from SES_UF2 su
          left join Ses_cias sc on su.coenti = sc.Coenti 
          where su.ramos in (1161, 1130, 1101, 1102) and sc.Noenti like '%BRASILSEG%'
          GROUP BY su.UF, su.damesano 
          order by su.UF, su.damesano "

bd <- dbConnect(SQLite(), "bdSES.sqlite")
dados_Uf <- dbGetQuery(bd, query)
dbDisconnect(bd)


query <- "SELECT 
            ss.damesano
            , SUM(ss.premio_direto) as premio_dir 
            , sum(ss.sinistro_direto) as sin_dir
            , sum(ss.sinistro_direto)/SUM(ss.premio_direto) as razao
          from Ses_seguros ss
          left join Ses_cias sc on ss.coenti = sc.Coenti 
          where ss.coramo  in (1161, 1130, 1101, 1102) and sc.Noenti like '%BRASILSEG%'
          GROUP BY ss.damesano 
          order by ss.damesano  "

bd <- dbConnect(SQLite(), "bdSES.sqlite")
dados_mes <- dbGetQuery(bd, query)
dbDisconnect(bd)
