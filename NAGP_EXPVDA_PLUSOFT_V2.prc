CREATE OR REPLACE PROCEDURE CONSINCO.NAGP_EXPVDA_PLUSOFT_V2 (vsDtaInicial DATE, vsDtaFinal DATE) IS

    v_file UTL_FILE.file_type;
    v_line VARCHAR2(32767);
    v_Targetcharset varchar2(40 BYTE);
    v_Dbcharset varchar2(40 BYTE);
    v_Cabecalho VARCHAR2(4000);
    v_Periodo VARCHAR2(10);
    v_buffer CLOB;
    v_chunk_size CONSTANT PLS_INTEGER := 32000; -- Ajuste conforme necessário

BEGIN

    FOR t IN (SELECT X.ANOMESDESCRICAO,
                   MIN(TO_DATE(LAST_DAY(ADD_MONTHS(X.DTA, -1)) + 1, 'DD/MM/RRRR')) DTA_INICIAL,
                   MAX(TO_DATE(LAST_DAY(X.DTA), 'DD/MM/RRRR')) DTA_FINAL
              FROM DIM_TEMPO X
             WHERE X.DTA BETWEEN vsDtaInicial AND vsDtaFinal
             GROUP BY X.ANOMESDESCRICAO
             ORDER BY 2)

    LOOP
          
    BEGIN
      
    DELETE FROM CONSINCO.NAGT_TMP_PLUSOFT WHERE 1=1;
    COMMIT;
      
    INSERT /*+ APPEND */ INTO CONSINCO.NAGT_TMP_PLUSOFT 
    
    SELECT /*+OPTIMIZER_FEATURES_ENABLE('11.2.0.4')*/
     (Z.CPFCNPJ), X.SEQNF, X.NROEMPRESA, X.DTAMOVIMENTO
      FROM MFL_DOCTOFISCAL X INNER JOIN PDV_DOCTO Y ON X.NFECHAVEACESSO = Y.CHAVEACESSO
                                                   AND Y.DTAMOVIMENTO = X.DTAMOVIMENTO
                                                   AND X.NROEMPRESA = Y.NROEMPRESA
                                                   AND X.NUMERODF = Y.NUMERODF
                                                   AND X.SERIEDF = Y.SERIEDF
                             INNER JOIN PDV_DESCONTO Z ON (Z.SEQDOCTO = Y.SEQDOCTO)
                             
    WHERE X.DTAMOVIMENTO BETWEEN T.DTA_INICIAL AND T.DTA_FINAL
    
    UNION ALL
    
    SELECT /*+OPTIMIZER_FEATURES_ENABLE('11.2.0.4')*/
     TO_NUMBER(XDF.CNPJCPF), X.SEQNF, X.NROEMPRESA, X.DTAMOVIMENTO
      FROM MFL_DOCTOFISCAL X INNER JOIN MFL_DOCTOFIDELIDADE XDF ON XDF.SEQNF = X.SEQNF
                             
    WHERE X.DTAMOVIMENTO BETWEEN T.DTA_INICIAL AND T.DTA_FINAL
    ;                   
    COMMIT;
        
    END;

    V_Periodo := REPLACE(t.ANOMESDESCRICAO, '/','_');

    -- Abre o arquivo para escrita
    v_file := UTL_FILE.fopen('/u02/app_acfs/arquivos/plusoft', 'Ext_v2_Vda_'||v_Periodo||'.csv', 'w', 32767);

    -- Pega o nome das colunas para inserir no cabecalho pq tenho preguica
    SELECT LISTAGG(COLUMN_NAME,';') WITHIN GROUP (ORDER BY COLUMN_ID)
      INTO v_Cabecalho
      FROM ALL_TAB_COLUMNS@CONSINCODW A
     WHERE A.table_name = 'NAGV_PLUSOFT_VENDAS'
       AND COLUMN_NAME != 'DATA';

    -- Escreve o cabe¿alho do CSV
    UTL_FILE.put_line(v_file, v_Cabecalho);

    -- Executa a query e escreve os resultados

      FOR vda IN (SELECT /*+OPTIMIZER_FEATURES_ENABLE('11.2.0.4')*/VD.IDPESSOA, 
                                                                   VD.IDFILIAL,
                                                                   VD.IDCUPOM,
                                                                   VD.IDPRODUTO,
                                                                   VD.DATVENDA,
                                                                   VD.NUMQTDVENDIDA,
                                                                   VD.DATA,
                                                                   VD.VLRPRECOVENDAUNITARIO,
                                                                   VD.VLRPRECOPDVUNITARIO,
                                                                   VD.VLRDESCONTOUNITARIO,
                                                                   VD.VLRMARGEMPDV,
                                                                   VD.TXTCANALVENDAS,
                                                                   VD.TXTTIPOVENDA,
                                                                   VD.IDFORMAPAGTO,
                                                                   VD.TXTFORMAPAGTO 
                                                                                         
                                                                      FROM CONSINCO.NAGV_PLUSOFT_VENDAS VD 
                                                                     WHERE VD.DATA BETWEEN t.DTA_INICIAL AND t.DTA_FINAL)

      LOOP

        v_line := vda.IDPESSOA||';'||vda.IDFILIAL||';'||vda.IDCUPOM||';'||vda.IDPRODUTO||';'||vda.DATVENDA||';'||vda.NUMQTDVENDIDA||';'||
                  vda.VLRPRECOVENDAUNITARIO||';'||vda.VLRPRECOPDVUNITARIO||';'||vda.VLRDESCONTOUNITARIO||';'||vda.VLRMARGEMPDV||';'||
                  vda.TXTCANALVENDAS||';'||vda.TXTTIPOVENDA||';'||vda.IDFORMAPAGTO||';'||vda.TXTFORMAPAGTO;

        v_buffer := v_buffer || v_line || CHR(10); -- Adiciona nova linha ao buffer
        
        IF LENGTH(v_buffer) > v_chunk_size THEN
            UTL_FILE.put_line(v_file, v_buffer); -- Escreva o buffer no arquivo
            v_buffer := ''; -- Limpe o buffer
            
        END IF;
        
    END LOOP;

    -- Fecha o arquivo
    UTL_FILE.fclose(v_file);

    END LOOP;

EXCEPTION

    WHEN OTHERS THEN
        IF UTL_FILE.is_open(v_file) THEN
            UTL_FILE.fclose(v_file);
        END IF;
        RAISE;

END;
