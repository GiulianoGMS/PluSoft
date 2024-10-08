ALTER SESSION SET CURRENT_SCHEMA = CONSINCO;

CREATE OR REPLACE VIEW CONSINCO.NAGV_PLUSOFT_FILIAL AS 

SELECT NROEMPRESA IDFILIAL, X.FANTASIA TXTNOMEFANTASIA, NULL DATABERTURA,
       CASE WHEN G.LOGRADOURO LIKE 'AV%' THEN 'AVENIDA'
            WHEN G.LOGRADOURO LIKE 'ESTRADA%' THEN 'ESTRADA'
            WHEN G.LOGRADOURO LIKE 'PRACA%' THEN 'PRACA'
            WHEN G.LOGRADOURO LIKE 'RODOVIA%' THEN 'RODOVIA'
            WHEN G.LOGRADOURO LIKE 'VIA%' THEN 'VIA'
              ELSE 'RUA' END TXTTIPOLOGRADOURO, 
       G.LOGRADOURO TXTLOGRADOURO,
       G.NROLOGRADOURO NUMNUMERO,
       G.CMPLTOLOGRADOURO TXTCOMPLEMENTO,
       G.BAIRRO TXTBAIRRO,
       G.CIDADE TXTCIDADE,
       G.CEP NUMCEP,
       G.UF TXTUF,
       X.LATITUDE NUMLAT,
       X.LONGITUDE NUMLONG,
       TIPO TXTTIPOFILIAL,
       CODREGIONAL IDREGIONAL,
       DESCREGIONAL TXTREGIONAL,
       NULL TXTCLUSTER,
       METRAGEM NUMMETRAGEM,
       AREADEVENDAS NUMAREAVENDA,
       FUNCIONARIOS QTDFUNCIONARIOS,
       'PROPRIA' TXTFRANQUIA,
       DECODE(X.OPERACAOINICIADA, 'S',1,'N',0) FLGATIVA,
       CASE WHEN X.TIPOCANAL LIKE 'Sem Classifica%' THEN 'Sem Classificacao' ELSE X.TIPOCANAL END TXTCANALATENDIMENTO
       
  FROM DWNAGT_DADOSEMPRESA@BI X INNER JOIN CONSINCO.GE_PESSOA G ON G.SEQPESSOA = X.NROEMPRESA

-- NO DW

CREATE OR REPLACE VIEW CONSINCODW.NAGV_PLUSOFT_FILIAL AS 

SELECT NROEMPRESA IDFILIAL, X.FANTASIA TXTNOMEFANTASIA, NULL DATABERTURA,
       CASE WHEN G.LOGRADOURO LIKE 'AV%' THEN 'AVENIDA'
            WHEN G.LOGRADOURO LIKE 'ESTRADA%' THEN 'ESTRADA'
            WHEN G.LOGRADOURO LIKE 'PRACA%' THEN 'PRACA'
            WHEN G.LOGRADOURO LIKE 'RODOVIA%' THEN 'RODOVIA'
            WHEN G.LOGRADOURO LIKE 'VIA%' THEN 'VIA'
              ELSE 'RUA' END TXTTIPOLOGRADOURO, 
       G.LOGRADOURO TXTLOGRADOURO,
       G.NROLOGRADOURO NUMNUMERO,
       G.CMPLTOLOGRADOURO TXTCOMPLEMENTO,
       G.BAIRRO TXTBAIRRO,
       G.CIDADE TXTCIDADE,
       G.CEP NUMCEP,
       G.UF TXTUF,
       X.LATITUDE NUMLAT,
       X.LONGITUDE NUMLONG,
       TIPO TXTTIPOFILIAL,
       CODREGIONAL IDREGIONAL,
       DESCREGIONAL TXTREGIONAL,
       NULL TXTCLUSTER,
       METRAGEM NUMMETRAGEM,
       AREADEVENDAS NUMAREAVENDA,
       FUNCIONARIOS QTDFUNCIONARIOS,
       'PROPRIA' TXTFRANQUIA,
       DECODE(X.OPERACAOINICIADA, 'S',1,'N',0) FLGATIVA,
       CASE WHEN X.TIPOCANAL LIKE 'Sem Classifica%' THEN 'Sem Classificacao' ELSE X.TIPOCANAL END TXTCANALATENDIMENTO
       
  FROM DWNAGT_DADOSEMPRESA X INNER JOIN GE_PESSOA@LINK_C5 G ON G.SEQPESSOA = X.NROEMPRESA
