package com.mesalab.executor.exception;

import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.enums.BaseResultEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * 业务异常
 *
 * @author dazzlzy
 * @date 2018/3/22
 */
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BusinessException extends RuntimeException {

    /**
     * 异常代码
     */
    private int code = BaseResultEnum.SERVER_ERROR.getCode();
    /**
     * 异常信息
     */
    private String message;

    public BusinessException(String message) {
        this.code = BaseResultEnum.SERVER_ERROR.getCode();
        this.message = message;
    }


    public BusinessException(String message, Throwable e) {
        super(message, e);
        this.code = BaseResultEnum.SERVER_ERROR.getCode();
    }

    public BusinessException(int code, String message, Throwable e) {
        super(message, e);
        this.code = code;
    }
    
    public BusinessException(Throwable e) {
        super(e.getMessage(), e);
        this.code = BaseResultEnum.SERVER_ERROR.getCode();
        this.message = e.getMessage();
    }

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMessage() {
		return JobUtil.formatJobLog(message);
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
