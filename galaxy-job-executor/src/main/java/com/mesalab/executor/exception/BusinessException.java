package com.mesalab.executor.exception;

import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.enums.BaseResultEnum;
import lombok.*;

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
    private int errorCode = BaseResultEnum.SERVER_ERROR.getCode();
    /**
     * 异常信息
     */
    private String errorMessage;

    public BusinessException(String errorMessage) {
        this.errorCode = BaseResultEnum.SERVER_ERROR.getCode();
        this.errorMessage = errorMessage;
    }


    public BusinessException(String errorMessage, Throwable e) {
        super(errorMessage, e);
        this.errorCode = BaseResultEnum.SERVER_ERROR.getCode();
    }

    public BusinessException(int errorCode, String errorMessage, Throwable e) {
        super(errorMessage, e);
        this.errorCode = errorCode;
    }
    
    public BusinessException(Throwable e) {
        super(e.getMessage(), e);
        this.errorCode = BaseResultEnum.SERVER_ERROR.getCode();
        this.errorMessage = e.getMessage();
    }

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrorMessage() {
		return JobUtil.formatJobLog(errorMessage);
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

}
