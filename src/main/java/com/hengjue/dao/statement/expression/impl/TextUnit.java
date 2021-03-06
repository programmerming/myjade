package com.hengjue.dao.statement.expression.impl;

import com.hengjue.dao.statement.expression.ExprResolver;
import com.hengjue.dao.statement.expression.ExqlContext;
import com.hengjue.dao.statement.expression.ExqlUnit;

/**
 * 原样输出文本的语句单元, 通常是语句中不含表达式的部分。
 * 
 * @author han.liao
 */
public class TextUnit implements ExqlUnit {

    private final String text;

    /**
     * 构造输出文本的语句单元。
     * 
     * @param text - 输出的文本
     */
    public TextUnit(String text) {
        this.text = text;
    }

    @Override
    public boolean isValid(ExprResolver exprResolver) {

        // 文本始终有效
        return true;
    }

    @Override
    public void fill(ExqlContext exqlContext, ExprResolver exprResolver) throws Exception {

        // 输出未经转义的文本
        exqlContext.fillText(text);
    }
}
