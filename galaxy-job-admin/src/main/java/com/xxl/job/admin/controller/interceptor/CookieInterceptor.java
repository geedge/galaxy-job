package com.xxl.job.admin.controller.interceptor;

import com.xxl.job.admin.core.util.FtlUtil;
import com.xxl.job.admin.core.util.I18nUtil;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * push cookies to model as cookieMap
 *
 * @author xuxueli 2015-12-12 18:09:04
 */
@Component
public class CookieInterceptor extends HandlerInterceptorAdapter {

	@Override
	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
			ModelAndView mv) throws Exception {

		// cookie
		if (mv!=null && request.getCookies()!=null && request.getCookies().length>0) {
//			HashMap<String, Cookie> cookieMap = new HashMap<String, Cookie>();
//			for (Cookie ck : request.getCookies()) {
//				cookieMap.put(ck.getName(), ck);
//			}
//			mv.addObject("cookieMap", cookieMap);
		}

		// static method
		if (mv != null) {
			mv.addObject("I18nUtil", FtlUtil.generateStaticModel(I18nUtil.class.getName()));
		}
		
		super.postHandle(request, response, handler, mv);
	}
	
}
