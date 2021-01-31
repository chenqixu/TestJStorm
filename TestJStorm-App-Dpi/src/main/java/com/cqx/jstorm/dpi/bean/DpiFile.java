package com.cqx.jstorm.dpi.bean;

import com.cqx.jstorm.dpi.utils.TimeUtil;

import java.util.List;

/**
 * DpiFile
 *
 * @author chenqixu
 */
public class DpiFile implements Comparable<DpiFile> {
    private String filename;
    private String date;
    private String nameSeparator;
    private String dateLocal;
    private String endwith;
    private TypeDef typeDef;
    private List<TypeDef> typeDefList;

    public DpiFile(String filename, String nameSeparator, String dateLocal, String endwith, List<TypeDef> typeDefList) {
        this.filename = filename;
        this.nameSeparator = nameSeparator;
        this.dateLocal = dateLocal;
        this.endwith = endwith;
        this.typeDefList = typeDefList;
        init();
    }

    public void init() {
        // 计算文件名中的时间
        String[] dateLocals = dateLocal.split(",", -1);
        String[] names = filename.split(nameSeparator, -1);
        String name_date = "";
        for (String _dateLocals : dateLocals) {
            if (names.length > Integer.valueOf(_dateLocals)) {
                name_date += names[Integer.valueOf(_dateLocals)];
            } else {
                return;
            }
        }
        date = name_date.replace(endwith, "");
        // 匹配关键字
        for (TypeDef typeDef : typeDefList) {
            if (filename.contains(typeDef.getKeyWord())) {
                this.typeDef = typeDef;
                break;
            }
        }
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public TypeDef getTypeDef() {
        return typeDef;
    }

    public void setTypeDef(TypeDef typeDef) {
        this.typeDef = typeDef;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public int compareTo(DpiFile o) {
        return TimeUtil.timeComparison(getDate(), o.getDate());
    }

    @Override
    public String toString() {
        return "[ DpiFile]，filename：" + filename + "，date：" + date + "，typeDef：" + typeDef;
    }
}
