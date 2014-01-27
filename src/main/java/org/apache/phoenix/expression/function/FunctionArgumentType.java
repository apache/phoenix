package org.apache.phoenix.expression.function;

import java.text.DecimalFormat;
import java.text.Format;

import org.apache.phoenix.util.DateUtil;

public enum FunctionArgumentType {
    TEMPORAL {
        @Override
        public Format getFormatter(String format) {
            return DateUtil.getDateFormatter(format);
        }
    }, 
    NUMERIC {
        @Override
        public Format getFormatter(String format) {
            return new DecimalFormat(format);
        }
    },
    CHAR {
        @Override
        public Format getFormatter(String format) {
            return getDecimalFormat(format);
        }
    };        

    public abstract Format getFormatter(String format);
    
    private static DecimalFormat getDecimalFormat(String formatString) {
        DecimalFormat result = new DecimalFormat(formatString);
        result.setParseBigDecimal(true);
        return result;
    }
}
