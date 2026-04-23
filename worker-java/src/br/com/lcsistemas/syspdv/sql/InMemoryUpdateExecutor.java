package br.com.lcsistemas.syspdv.sql;

import java.util.*;
import java.util.logging.Logger;
import java.util.regex.*;

/**
 * Executa UPDATEs simples diretamente no SqlMemoryStore, eliminando a
 * necessidade de gerar UPDATEs no arquivo SQL de saída.
 *
 * Padrões suportados (aplicados in-memory):
 *   SET col = 'val'  [WHERE col IS NULL]
 *   SET col = N      [WHERE col IS NULL]
 *   SET col = ''     [WHERE col IS NULL | OR col = '']
 *   SET col = otherCol  [WHERE col IS NULL OR col = '']
 *   SET col = UPPER(col) / TRIM(col) / TRIM(UPPER(col))
 *   SET col = REPLACE(col, 'from', 'to')
 *   SET col = ''  WHERE col LIKE '%X%'
 *   SET col = 'X' WHERE col LIKE '%Y%'
 *   SET col = 'F'  WHERE LENGTH(otherCol) <= N
 *   SET col = 'J'  WHERE LENGTH(otherCol) > N
 *   SET col = 0   WHERE col < 0
 *   SET col = 0   WHERE col = 0 (sem efeito mas não quebra)
 *   SET col = ''  WHERE col IS NULL OR col = 'X'
 *   SET col = otherCol WHERE col IS NULL OR col = ''  (copy)
 *   SET col = 'X' WHERE otherCol = ''  (condicional em outra coluna)
 *   SET col = 1   WHERE col IS NULL
 *
 * Padrões NÃO suportados (devem permanecer como deferred SQL):
 *   CONCAT / SUBSTRING (formatação CNPJ)
 *   CONVERT(col, DECIMAL) / cálculos de margem
 *   NOW() / DATE(NOW())
 *   LPAD()
 *   IF()
 *   SUBSTRING_INDEX()
 *   UPDATE com JOIN
 *   INNER JOIN ... SET
 *   Múltiplos SET: SET a=0, b=0
 */
public class InMemoryUpdateExecutor {

    private static final Logger LOG = Logger.getLogger(InMemoryUpdateExecutor.class.getName());

    private final SqlMemoryStore store;

    // ── Patterns ──────────────────────────────────────────────────────────────

    // UPDATE [schema.]table SET col = ... [WHERE ...]
    private static final Pattern P_TABLE = Pattern.compile(
        "^UPDATE\\s+(?:\\w+\\.)?`?(\\w+)`?\\s+SET\\s+(.+?)(?:\\s+WHERE\\s+(.+))?$",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // SET col = 'val'
    private static final Pattern P_SET_STR = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*'(.*)'$", Pattern.DOTALL);

    // SET col = number (including decimals like 0.000)
    private static final Pattern P_SET_NUM = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*(-?[0-9]+(?:\\.[0-9]+)?)$");

    // SET col = otherCol (copy)
    private static final Pattern P_SET_COL = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*`?(\\w+)`?$");

    // SET col = UPPER(col)
    private static final Pattern P_SET_UPPER = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*UPPER\\(`?(\\w+)`?\\)$", Pattern.CASE_INSENSITIVE);

    // SET col = TRIM(col)
    private static final Pattern P_SET_TRIM = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*TRIM\\(`?(\\w+)`?\\)$", Pattern.CASE_INSENSITIVE);

    // SET col = TRIM(UPPER(col)) or UPPER(TRIM(col))
    private static final Pattern P_SET_TRIM_UPPER = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*(?:TRIM\\(UPPER\\(|UPPER\\(TRIM\\()`?(\\w+)`?\\)\\)$",
        Pattern.CASE_INSENSITIVE);

    // SET col = REPLACE(col, 'from', 'to')
    private static final Pattern P_SET_REPLACE = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*REPLACE\\(`?(\\w+)`?,\\s*'((?:[^'\\\\]|\\\\.)*)'\\s*,\\s*'((?:[^'\\\\]|\\\\.)*)'\\)$",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // WHERE col IS NULL
    private static final Pattern P_WHERE_IS_NULL = Pattern.compile(
        "^`?(\\w+)`?\\s+IS\\s+NULL$", Pattern.CASE_INSENSITIVE);

    // WHERE col IS NULL OR col = ''
    private static final Pattern P_WHERE_NULL_OR_EMPTY = Pattern.compile(
        "^`?(\\w+)`?\\s+IS\\s+NULL\\s+OR\\s+`?\\1`?\\s*=\\s*''$", Pattern.CASE_INSENSITIVE);

    // WHERE col = 'val'
    private static final Pattern P_WHERE_EQ_STR = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*'([^']*)'$", Pattern.CASE_INSENSITIVE);

    // WHERE col = N
    private static final Pattern P_WHERE_EQ_NUM = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*(-?[0-9]+(?:\\.[0-9]+)?)$");

    // WHERE col IS NULL OR col = ''  (generic — separate col check)
    private static final Pattern P_WHERE_NULL_OR_EMPTY2 = Pattern.compile(
        "^`?(\\w+)`?\\s+IS\\s+NULL\\s+OR\\s+`?(\\w+)`?\\s*=\\s*''$", Pattern.CASE_INSENSITIVE);

    // WHERE col LIKE '%X%'
    private static final Pattern P_WHERE_LIKE = Pattern.compile(
        "^`?(\\w+)`?\\s+LIKE\\s+'%([^']+)%'$", Pattern.CASE_INSENSITIVE);

    // WHERE col < N
    private static final Pattern P_WHERE_LT = Pattern.compile(
        "^`?(\\w+)`?\\s*<\\s*(-?[0-9]+(?:\\.[0-9]+)?)$");

    // WHERE col <= 0 / > 0 (for tipo detection from cnpj length — but we handle LENGTH separately)
    // WHERE LENGTH(col) <= N  or  > N
    private static final Pattern P_WHERE_LENGTH_LE = Pattern.compile(
        "^LENGTH\\(`?(\\w+)`?\\)\\s*<=\\s*(\\d+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_WHERE_LENGTH_GT = Pattern.compile(
        "^LENGTH\\(`?(\\w+)`?\\)\\s*>\\s*(\\d+)$", Pattern.CASE_INSENSITIVE);

    // WHERE otherCol = 'val' OR otherCol IS NULL  — e.g. WHERE fone IS NULL OR fone = ''
    private static final Pattern P_WHERE_NULL_OR_EMPTY_FONE = Pattern.compile(
        "^`?(\\w+)`?\\s+IS\\s+NULL\\s+OR\\s+`?(\\w+)`?\\s*=\\s*''$", Pattern.CASE_INSENSITIVE);

    // WHERE col = '' (just empty)
    private static final Pattern P_WHERE_EMPTY = Pattern.compile(
        "^`?(\\w+)`?\\s*=\\s*''$");

    // Detect complex patterns that must stay as deferred SQL
    private static final Pattern P_COMPLEX = Pattern.compile(
        "CONCAT|SUBSTRING|CONVERT|NOW\\(|DATE\\(|LPAD|IF\\(|SUBSTRING_INDEX|"
        + "INNER\\s+JOIN|LEFT\\s+JOIN|JOIN\\s+\\w|\\(preco_|\\(estoque|margem_lucro.*=.*preco|"
        + "DECIMAL\\(|SET\\s+\\w+\\s*=.*,\\s*\\w+\\s*=",  // multiple SET assignments
        Pattern.CASE_INSENSITIVE);

    // ── Constructor ───────────────────────────────────────────────────────────

    public InMemoryUpdateExecutor(SqlMemoryStore store) {
        this.store = store;
    }

    /**
     * Tenta aplicar o UPDATE diretamente no store.
     * @return true se foi aplicado in-memory; false se deve permanecer como deferred SQL.
     */
    public boolean apply(String sql) {
        if (sql == null || sql.trim().isEmpty()) return true;
        String norm = sql.trim().replaceAll(";$", "").trim();

        // Rejeita padrões complexos imediatamente
        if (P_COMPLEX.matcher(norm).find()) return false;

        Matcher m = P_TABLE.matcher(norm);
        if (!m.matches()) return false;

        String table  = m.group(1).toLowerCase();
        String setPart = m.group(2).trim();
        String where  = m.group(3) != null ? m.group(3).trim() : null;

        List<Map<String,Object>> rows = store.selectAll(table);
        if (rows == null || rows.isEmpty()) return true; // nada para alterar

        // Resolve o valor a setar
        SetAction action = parseSetAction(setPart);
        if (action == null) return false; // padrão SET não reconhecido

        // Resolve o predicado WHERE
        RowPredicate predicate = (where == null || where.isEmpty())
            ? row -> true
            : parseWhere(where);
        if (predicate == null) return false; // WHERE não reconhecido

        // Aplica
        for (Map<String,Object> row : rows) {
            if (predicate.test(row)) {
                action.apply(row);
            }
        }
        return true;
    }

    // ── Predicados WHERE ─────────────────────────────────────────────────────

    @FunctionalInterface
    interface RowPredicate { boolean test(Map<String,Object> row); }

    private RowPredicate parseWhere(String where) {
        // WHERE col IS NULL
        Matcher m = P_WHERE_IS_NULL.matcher(where);
        if (m.matches()) {
            String col = m.group(1).toLowerCase();
            return row -> row.get(col) == null;
        }

        // WHERE col IS NULL OR col = ''
        m = P_WHERE_NULL_OR_EMPTY2.matcher(where);
        if (m.matches()) {
            String col1 = m.group(1).toLowerCase();
            String col2 = m.group(2).toLowerCase();
            // same col or different col — handle both
            return row -> {
                Object v1 = row.get(col1);
                Object v2 = row.get(col2);
                return v1 == null || "".equals(String.valueOf(v1))
                    || v2 == null || "".equals(String.valueOf(v2));
            };
        }

        // WHERE col = 'val'
        m = P_WHERE_EQ_STR.matcher(where);
        if (m.matches()) {
            String col = m.group(1).toLowerCase();
            String val = m.group(2);
            return row -> val.equals(str(row.get(col)));
        }

        // WHERE col = N
        m = P_WHERE_EQ_NUM.matcher(where);
        if (m.matches()) {
            String col = m.group(1).toLowerCase();
            double val = Double.parseDouble(m.group(2));
            return row -> numEq(row.get(col), val);
        }

        // WHERE col = ''
        m = P_WHERE_EMPTY.matcher(where);
        if (m.matches()) {
            String col = m.group(1).toLowerCase();
            return row -> "".equals(str(row.get(col)));
        }

        // WHERE col LIKE '%X%'
        m = P_WHERE_LIKE.matcher(where);
        if (m.matches()) {
            String col  = m.group(1).toLowerCase();
            String sub  = m.group(2).toUpperCase();
            return row -> {
                Object v = row.get(col);
                return v != null && v.toString().toUpperCase().contains(sub);
            };
        }

        // WHERE col < N
        m = P_WHERE_LT.matcher(where);
        if (m.matches()) {
            String col = m.group(1).toLowerCase();
            double val = Double.parseDouble(m.group(2));
            return row -> {
                Object v = row.get(col);
                if (v == null) return false;
                try { return Double.parseDouble(v.toString()) < val; }
                catch (NumberFormatException e) { return false; }
            };
        }

        // WHERE LENGTH(col) <= N
        m = P_WHERE_LENGTH_LE.matcher(where);
        if (m.matches()) {
            String col = m.group(1).toLowerCase();
            int    len = Integer.parseInt(m.group(2));
            return row -> {
                Object v = row.get(col);
                return v != null && v.toString().length() <= len;
            };
        }

        // WHERE LENGTH(col) > N
        m = P_WHERE_LENGTH_GT.matcher(where);
        if (m.matches()) {
            String col = m.group(1).toLowerCase();
            int    len = Integer.parseInt(m.group(2));
            return row -> {
                Object v = row.get(col);
                return v != null && v.toString().length() > len;
            };
        }

        // WHERE tipo <> 'E'
        Matcher mne = Pattern.compile(
            "^`?(\\w+)`?\\s*<>\\s*'([^']*)'$", Pattern.CASE_INSENSITIVE).matcher(where);
        if (mne.matches()) {
            String col = mne.group(1).toLowerCase();
            String val = mne.group(2);
            return row -> !val.equals(str(row.get(col)));
        }

        // WHERE tipo = 'E'  (handled by P_WHERE_EQ_STR above)
        // WHERE nome IS NULL OR nome = '' — already handled by P_WHERE_NULL_OR_EMPTY2

        return null; // não reconhecido
    }

    // ── Ações SET ─────────────────────────────────────────────────────────────

    @FunctionalInterface
    interface SetAction { void apply(Map<String,Object> row); }

    private SetAction parseSetAction(String set) {
        // SET col = TRIM(UPPER(col)) / UPPER(TRIM(col))
        Matcher m = P_SET_TRIM_UPPER.matcher(set);
        if (m.matches()) {
            String dst = m.group(1).toLowerCase();
            String src = m.group(2).toLowerCase();
            return row -> {
                Object v = row.get(src);
                row.put(dst, v == null ? "" : v.toString().trim().toUpperCase());
            };
        }

        // SET col = UPPER(col)
        m = P_SET_UPPER.matcher(set);
        if (m.matches()) {
            String dst = m.group(1).toLowerCase();
            String src = m.group(2).toLowerCase();
            return row -> {
                Object v = row.get(src);
                row.put(dst, v == null ? null : v.toString().toUpperCase());
            };
        }

        // SET col = TRIM(col)
        m = P_SET_TRIM.matcher(set);
        if (m.matches()) {
            String dst = m.group(1).toLowerCase();
            String src = m.group(2).toLowerCase();
            return row -> {
                Object v = row.get(src);
                row.put(dst, v == null ? null : v.toString().trim());
            };
        }

        // SET col = REPLACE(col, 'from', 'to')
        m = P_SET_REPLACE.matcher(set);
        if (m.matches()) {
            String dst  = m.group(1).toLowerCase();
            String src  = m.group(2).toLowerCase();
            String from = unescape(m.group(3));
            String to   = unescape(m.group(4));
            return row -> {
                Object v = row.get(src);
                if (v != null) row.put(dst, v.toString().replace(from, to));
            };
        }

        // SET col = 'val'
        m = P_SET_STR.matcher(set);
        if (m.matches()) {
            String dst = m.group(1).toLowerCase();
            String val = unescape(m.group(2));
            return row -> row.put(dst, val);
        }

        // SET col = number
        m = P_SET_NUM.matcher(set);
        if (m.matches()) {
            String dst = m.group(1).toLowerCase();
            String num = m.group(2);
            // Parse as int if no decimal part, else double
            Object val = num.contains(".") ? Double.parseDouble(num)
                                           : Long.parseLong(num);
            return row -> row.put(dst, val);
        }

        // SET col = otherCol (copy)
        m = P_SET_COL.matcher(set);
        if (m.matches()) {
            String dst = m.group(1).toLowerCase();
            String src = m.group(2).toLowerCase();
            if (!dst.equals(src)) {
                return row -> row.put(dst, row.get(src));
            }
        }

        return null;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String str(Object v) {
        return v == null ? null : v.toString();
    }

    private static boolean numEq(Object v, double target) {
        if (v == null) return false;
        try { return Double.parseDouble(v.toString()) == target; }
        catch (NumberFormatException e) { return false; }
    }

    /** Desfaz escapes SQL simples (\' → ') */
    private static String unescape(String s) {
        return s.replace("\\'", "'").replace("\\\\", "\\");
    }
}
