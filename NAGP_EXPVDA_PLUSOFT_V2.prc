CREATE OR REPLACE PROCEDURE NAGP_EXPVDA_PLUSOFT_V2 (vsDtaInicial DATE, vsDtaFinal DATE) IS

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
    /* A tabela abaixo irá agrupar os CPFs/CNPJs para serem utilizados na view que gera os arquivos */
    BEGIN
      
    DELETE FROM NAGT_TMP_PLUSOFT WHERE 1=1;
    COMMIT;
      
    INSERT /*+ APPEND */ INTO CONSINCO.NAGT_TMP_PLUSOFT 
    /* Cupom PDV Remarca */
    SELECT /*+OPTIMIZER_FEATURES_ENABLE('11.2.0.4')*/
           Z.CPFCNPJ CPF, X.SEQNF, X.NROEMPRESA, X.DTAMOVIMENTO, NVL(NAGF_ECOMM_CANALVENDA(X.NROEMPRESA, X.NUMERODF, X.DTAMOVIMENTO), P.PEDIDOID) PEDIDOID
      FROM MFL_DOCTOFISCAL X  INNER JOIN PDV_DOCTO Y ON X.NFECHAVEACESSO = Y.CHAVEACESSO
                                                    AND Y.DTAMOVIMENTO = X.DTAMOVIMENTO
                                                    AND X.NROEMPRESA = Y.NROEMPRESA
                                                    AND X.NUMERODF = Y.NUMERODF
                                                    AND X.SERIEDF = Y.SERIEDF
                              INNER JOIN PDV_DESCONTO Z ON (Z.SEQDOCTO = Y.SEQDOCTO)
                               LEFT JOIN MAD_PEDVENDA P ON P.NROPEDVENDA = X.NROPEDIDOVENDA
                             
    WHERE X.DTAMOVIMENTO BETWEEN t.DTA_INICIAL AND t.DTA_FINAL
      AND X.CODGERALOPER IN (37,48,123,610,615,613,810,916,910,911,76)
      AND X.SEQPESSOA > 999
    
    UNION
    /* Cupom PDV TOTVS */
    SELECT /*+OPTIMIZER_FEATURES_ENABLE('11.2.0.4')*/
           TO_NUMBER(XDF.CNPJCPF) CPF, X.SEQNF, X.NROEMPRESA, X.DTAMOVIMENTO, NVL(NAGF_ECOMM_CANALVENDA(X.NROEMPRESA, X.NUMERODF, X.DTAMOVIMENTO), P.PEDIDOID) PEDIDOID 
      FROM MFL_DOCTOFISCAL X  INNER JOIN MFL_DOCTOFIDELIDADE XDF ON XDF.SEQNF = X.SEQNF
                               LEFT JOIN MAD_PEDVENDA P ON P.NROPEDVENDA = X.NROPEDIDOVENDA
                              
    WHERE X.DTAMOVIMENTO BETWEEN t.DTA_INICIAL AND t.DTA_FINAL
      AND X.CODGERALOPER IN (37,48,123,610,615,613,810,916,910,911,76)
      AND X.SEQPESSOA > 999
    
    UNION
    /* Notas sem cupom */
    SELECT TO_NUMBER(G.NROCGCCPF||G.DIGCGCCPF) CPF, X.SEQNF, X.NROEMPRESA, X.DTAMOVIMENTO, PEDIDOID
      FROM MFL_DOCTOFISCAL X INNER JOIN GE_PESSOA G ON G.SEQPESSOA = X.SEQPESSOA
                             INNER JOIN MAD_PEDVENDA P ON P.NROPEDVENDA = X.NROPEDIDOVENDA
      
     WHERE X.DTAMOVIMENTO BETWEEN t.DTA_INICIAL AND t.DTA_FINAL
       AND X.CODGERALOPER IN (37,48,123,610,615,613,810,916,910,911,76)
       AND X.SEQPESSOA > 999;
                       
    COMMIT;
        
    END;

    V_Periodo := REPLACE(t.ANOMESDESCRICAO, '/','_');

    -- Abre o arquivo para escrita
    v_file := UTL_FILE.fopen('/u02/app_acfs/arquivos/plusoft', 'Ext_v3_Vda_'||v_Periodo||'.csv', 'w', 32767);

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
                                                                                         
                                                                      FROM CONSINCO.NAGV_PLUSOFT_VENDAS_EXT VD 
                                                                     WHERE VD.DATA BETWEEN t.DTA_INICIAL AND t.DTA_FINAL)

      LOOP

        v_line := vda.IDPESSOA||';'||vda.IDFILIAL||';'||vda.IDCUPOM||';'||vda.IDPRODUTO||';'||vda.DATVENDA||';'||vda.NUMQTDVENDIDA||';'||
                  vda.VLRPRECOVENDAUNITARIO||';'||vda.VLRPRECOPDVUNITARIO||';'||vda.VLRDESCONTOUNITARIO||';'||vda.VLRMARGEMPDV||';'||
                  vda.TXTCANALVENDAS||';'||vda.TXTTIPOVENDA||';'||vda.IDFORMAPAGTO||';'||vda.TXTFORMAPAGTO;

        v_buffer := v_buffer || v_line || CHR(10); -- Adiciona nova linha ao buffer
        
        IF LENGTH(v_buffer) > v_chunk_size THEN
            UTL_FILE.put_line(v_file, v_buffer); -- Escreve o buffer no arquivo
            v_buffer := ''; -- Limpe o buffer
            
        END IF;
        
    END LOOP;
    
    -- Grava o restante do buffer no final (burro esqueceu)
    IF v_buffer IS NOT NULL THEN
        UTL_FILE.put_line(v_file, v_buffer);
        v_buffer := '';
    END IF;
    
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
