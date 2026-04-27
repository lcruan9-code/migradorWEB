package br.com.lcsistemas.syspdv.step.ajuste;

import br.com.lcsistemas.syspdv.step.StepBase;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Classe base comum a todos os steps de ajuste pós-migração.
 *
 * Fornece:
 *  - Arrays SUBS e SUBS2 (substituições de caracteres / mojibake)
 *  - Todos os helpers estáticos (safeStr, safeDbl, safeInt, stripDoc,
 *    applyAllSubs, formatCnpj, formatCpf, nowTs, nowDate, round2, round3,
 *    lpad2, isNullOrEmpty, isNull, temFracionado)
 *  - commitSilent() para liberação de buffer de UNDO no H2
 *
 * Pré-requisito: AjustePosMigracaoStep deve ter executado antes.
 */
public abstract class AjusteBase extends StepBase {

    protected static final Logger LOG = Logger.getLogger(AjusteBase.class.getName());

    // =========================================================================
    //  SUBS — acentos/caracteres especiais (unicode direto)
    //  Aplicado em todos os campos de texto das tabelas migradas.
    // =========================================================================
    protected static final String[][] SUBS = {
        {"À","A"},{"Á","A"},{"Ã","A"},{"Â","A"},
        {"È","E"},{"É","E"},{"Ê","E"},
        {"Ì","I"},{"Í","I"},{"Î","I"},
        {"Ù","U"},{"Ú","U"},{"Û","U"},{"Ü","U"},
        {"Ç","C"},{"Õ","O"},
        {"A§A£","CA"},  // sequência corrompida → CA
        {"°"," "},{"º"," "},{"ª"," "},
        {"ƒ",""},   // ƒ
        {"‰",""},   // ‰
        {"•",""},   // •
        {"Š",""},   // Š
        {"‡",""},   // ‡
        {"  "," "}
    };

    // =========================================================================
    //  SUBS2 — mojibake (UTF-8 bytes lidos como Latin-1)
    //  Cobre casos de dupla-codificação frequentes em bancos exportados com charset errado.
    // =========================================================================
    protected static final String[][] SUBS2 = {
        {"Ã§Ã£","CA"},
        {"Ã§","C"},
        {"Ã£","A"},
        {"Ã¢","A"},
        {"Ã ","A"},
        {"Ã¡","A"},
        {"Ã©","E"},
        {"Ãª","E"},
        {"Ã­","I"},
        {"Ãº","U"},
        {"Ãµ","O"},
        {"Ã",""},
        {"Â°",""},
        {"Âº",""},
        {"<span id='product_description'>",""},
        {"\n\n",""},
        {"\n",""},
    };

    // =========================================================================
    //  HELPERS — data/hora
    // =========================================================================
    protected static String nowTs() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    protected static String nowDate() {
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    }

    // =========================================================================
    //  HELPERS — leitura segura de valores
    // =========================================================================
    protected static String safeStr(Object v) {
        return v == null ? "" : v.toString();
    }

    protected static double safeDbl(Object v) {
        if (v == null) return 0.0;
        try { return Double.parseDouble(v.toString()); }
        catch (NumberFormatException e) { return 0.0; }
    }

    protected static int safeInt(Object v) {
        if (v == null) return 0;
        try { return Integer.parseInt(v.toString()); }
        catch (NumberFormatException e) { return 0; }
    }

    protected static boolean isNullOrEmpty(Object v) {
        return v == null || v.toString().isEmpty();
    }

    protected static boolean isNull(Object v) {
        return v == null;
    }

    // =========================================================================
    //  HELPERS — formatação
    // =========================================================================
    protected static String stripDoc(Object v) {
        if (v == null) return "";
        return v.toString().replaceAll("[\\-/\\. ,]", "");
    }

    protected static boolean temFracionado(Object v) {
        if (v == null) return false;
        String s = v.toString();
        int dot = s.indexOf('.');
        if (dot < 0) return false;
        try { return Integer.parseInt(s.substring(dot + 1)) > 0; }
        catch (Exception e) { return false; }
    }

    protected static double round2(double d) {
        return Math.round(d * 100.0) / 100.0;
    }

    protected static double round3(double d) {
        return Math.round(d * 1000.0) / 1000.0;
    }

    protected static String lpad2(Object v) {
        if (v == null || v.toString().isEmpty()) return "07";
        try { return String.format("%02d", Integer.parseInt(v.toString())); }
        catch (NumberFormatException e) { return v.toString(); }
    }

    protected static String applyAllSubs(String s) {
        if (s == null) return s;
        for (String[] sub : SUBS)  s = s.replace(sub[0], sub[1]);
        for (String[] sub : SUBS2) s = s.replace(sub[0], sub[1]);
        return s;
    }

    protected static String formatCnpj(String d) {
        if (d == null || d.length() != 14) return d;
        return d.substring(0, 2) + "." + d.substring(2, 5) + "."
             + d.substring(5, 8) + "/" + d.substring(8, 12) + "-" + d.substring(12);
    }

    protected static String formatCpf(String d) {
        if (d == null || d.length() != 11) return d;
        return d.substring(0, 3) + "." + d.substring(3, 6) + "."
             + d.substring(6, 9) + "-" + d.substring(9);
    }

    // =========================================================================
    //  HELPER — commit intermediário (libera buffer de UNDO do H2)
    // =========================================================================
    protected void commitSilent(Connection c, String fase) {
        try {
            c.commit();
            LOG.fine("[" + getNome() + "] commit parcial OK: " + fase);
        } catch (SQLException e) {
            LOG.warning("[" + getNome() + "] commit parcial AVISO (" + fase + "): " + e.getMessage());
        }
    }
}
