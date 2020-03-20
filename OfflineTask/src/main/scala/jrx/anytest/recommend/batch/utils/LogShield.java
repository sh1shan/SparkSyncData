package jrx.anytest.recommend.batch.utils;

import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

/**
 * 日志脱敏信息屏蔽
 * 规则：
 * 1.客户名称屏蔽最后一个字符
 * 2.手机号显示前三位和后四位，家庭电话和办公电话屏蔽后四位
 * 3.身份证、港澳通行证、军官证等其他证件号码大于等于10位显示前4位，小于10位的只显示后四位
 * 4.联系地址禁止记录
 * 5.其他字段的屏蔽：
 * <p>
 * e.g:
 * 预编译多个自定义的脱敏规则：
 * String logDate={"ID_NBR":"13012345678910000X","NAME":"张三"，"MOBILE_NBR"："18288888888"，"tel":"8622222","ADDRESS":"广东省广州市"}
 * <p>
 * Pattern [] patterns=new patterns[]{
 * toPattern("\"ID_NBR"\:",IDCardRegex,"\""),
 * toPattern("\"NAME\":",nameRegx,"\""),
 * toPattern("\"MOBILE_NBR\":",phoneRegex,"\""),
 * toPattern("\"TEL\":",telPhoneRegex,"\""),
 * toPattern("\"ADDRESS\":",addressRegex,"\"")
 * }
 * String[] replacement=new String[]{
 * IDCardReplacement,nameReplacement,phoneReplacement,telephoneReplacement
 * }
 * spring 的用法：
 * private Pattern[] patterns;
 * private String[] replacements;
 *
 * @PostConstruct public void logShield(){
 * String end ="";
 * patterns =new Patterns[]{
 * toPattern("\"MOBILE_NBR\":\"":phoneRegx,end),
 * toPattern("\"GLOCAL_NAME\":\"",nameRegex,end)
 * replacements=new String[] {phoneReplacement,nameReplacement,IDCardReplacement};
 * }
 * }
 */
public class LogShield {
    /**
     * 正则表达式
     */
    public static final String PHONE_REGEX = "(\\+86|86)?(\\d{3})\\d{4}(\\d{4}\\s*)";
    public static final String ID_CARD_REGEX = "(\\d{4})\\d{2,10}(\\d{3})(\\d|\\w)(\\s*)";
    public static final String NAME_REGEX = "([a-zA-Z\u2E80-\u9FFF])+\\S(\\s*)";
    public static final String TELEPHONE_REGEX = "(\\d{3}-\\d{4}|\\d{4}-\\d{3}|\\d{4}|\\d{3})\\d{4}(\\s*)";
    public static final String ADDRESS_REGEX = "[0-9a-zA-Z\u2e80-\u9FFF\uFF10-\uFF19\uFF41-\uFF5A\uFF21-\uFF3A]+(\\s*)";

    /**
     * 编译正则
     */
    private static final Pattern PHONE_PATTERN = Pattern.compile(PHONE_REGEX);
    private static final Pattern ID_CARD_PSTTERN = Pattern.compile(ID_CARD_REGEX);
    private static final Pattern NAME_PSTTERN = Pattern.compile(NAME_REGEX);
    private static final Pattern TELEPHONE_PSTTERN = Pattern.compile(TELEPHONE_REGEX);
    private static final Pattern ADDRESS_PSTTERN = Pattern.compile(ADDRESS_REGEX);

    /**
     * 替换规则
     */
    public static final String PHONE_REPLACEMENT = "$1$2$3****$4$5";
    public static final String ID_CARD_REPLACEMENT = "$1$2**********$3$4$5$6";
    public static final String NAME_REPLACEMENT = "$1$2*$3$4";
    public static final String TELEPHONE_REPLACEMENT = "$1$2****$3$4";
    public static final String ADDRESSS_REPLACEMENT = "$1***$2$3";

    private static final String S = "(";
    private static final String E = ")";

    /**
     * 自定义规则
     *
     * @param str          字符串
     * @param patterns     正则
     * @param replacements 替换规则
     */
    public static String custom(String str, Pattern[] patterns, String[] replacements) {
        int len = replacements.length;
        checkLength(len, patterns.length);
        for (int i = 0; i < len; i++) {
            str = custom(str, patterns[i], replacements[i]);
        }
        return str;
    }

    public static String custom(String str, String regex, String replacement) {
        return custom(str, Pattern.compile(regex), replacement);
    }

    /**
     * 正则脱敏替换，可自定义正则替换
     * 若replacement使用的是此类内部的replacement，pattern需使用toPattern(...)生成
     *
     * @param str         字符串
     * @param pattern     正则
     * @param replacement 替换规则
     */
    public static String custom(String str, Pattern pattern, String replacement) {
        try {
            return pattern.matcher(str).replaceAll(replacement);
        } catch (Exception ignore) {
        }
        return "";
    }

    public static byte[] custom(byte[] byteArray, IndexPatten indexPatten, String replacement, String charsetName) {
        final byte[] shield = byteArray.clone();
        return byteCustom(shield, indexPatten, replacement, charsetName);
    }

    /**
     * 定长字节脱敏，多项
     *
     * @param byteArray    原始字节数组
     * @param indexPattens 正则
     * @param replacements 替换规则
     * @param charsetName  编码
     */
    public static byte[] custom(byte[] byteArray, IndexPatten[] indexPattens, String[] replacements, String charsetName) {
        int len = replacements.length;
        checkLength(len, indexPattens.length);
        final byte[] shield = byteArray.clone();
        for (int i = 0; i < len; i++) {
            byteCustom(shield, indexPattens[i], replacements[i], charsetName);
        }
        return shield;
    }

    public static String customToString(byte[] byteArray, IndexPatten[] indexPattens, String[] replacements, String charsetName) {
        try {
            return new String(custom(byteArray, indexPattens, replacements, charsetName), charsetName);
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    private static byte[] byteCustom(byte[] shield, IndexPatten indexPatten, String replacement, String charsetName) {
        try {
            byte[] shieldOneByte = new byte[indexPatten.length];
            System.arraycopy(shield, indexPatten.beginIndex, shieldOneByte, 0, indexPatten.length);
            String shieldOne = new String(shieldOneByte, charsetName).trim();
            //脱敏
            byte[] shieldOneNew = custom(shieldOne, indexPatten.pattern, replacement).getBytes(charsetName);
            byte empty = " ".getBytes(charsetName)[0];
            //放入
            for (int i = 0; i < shieldOneByte.length; i++) {
                shieldOneByte[i] = i < shieldOneNew.length ? shieldOneNew[i] : empty;
            }
            System.arraycopy(shieldOneByte, 0, shield, indexPatten.beginIndex, indexPatten.length);
        } catch (UnsupportedEncodingException ignore) {

        }
        return shield;
    }

    /**
     * 获取字符串脱敏规则
     *
     * @param prefix 字段前缀
     * @param regex  字段脱敏规则
     * @param suffix 字段后缀
     */
    public static Pattern toPattern(String prefix, String regex, String suffix) {
        String reRegex = S + prefix + E + regex + E + suffix + E;
        return Pattern.compile(reRegex);
    }

    /**
     * 获取定长字节脱敏规则
     *
     * @param beginIndex 字段所在字节数据的起始位置
     * @param length     字段的字节长度
     * @param regex      正则表达式
     */
    public static IndexPatten toPattern(int beginIndex, int length, String regex) {
        return new IndexPatten(beginIndex, length, toPattern("", regex, ""));
    }

    //TODO 手机号脱敏
    public static String phone(String str) {
        str = nullToEmpty(str);
        return custom(str, ADDRESS_PSTTERN, "***$1");
    }

    private static String nullToEmpty(String str) {
        return str == null ? "" : str.trim();
    }

    private static void checkLength(int len, int length) {
        if (len != length) {
            throw new IndexOutOfBoundsException("patterns与replacements长度不一致");
        }
    }

    public static class IndexPatten {
        private int beginIndex;
        private int length;
        private Pattern pattern;

        IndexPatten(int beginIndex, int length, Pattern pattern) {
            this.beginIndex = beginIndex;
            this.length = length;
            this.pattern = pattern;
        }
    }

}
