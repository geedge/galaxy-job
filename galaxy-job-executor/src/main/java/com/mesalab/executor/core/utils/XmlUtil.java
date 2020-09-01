package com.mesalab.executor.core.utils;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public class XmlUtil {

	/**
	 * xml转对象
	 * @param clazz
	 * @param xmlStr
	 * @return
	 */
	public static <T> T converXmlToBean(Class<T> clazz, String xmlStr) {
		T xmlObject = null;
		try {
			JAXBContext context = JAXBContext.newInstance(clazz);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			StringReader sr = new StringReader(xmlStr);
			xmlObject = (T) unmarshaller.unmarshal(sr);

		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return xmlObject;
	}

	/**
	 * 
	 * 对象转xml
	 * 
	 * @param obj
	 */
	public static String converToXml(Object obj) {
		StringWriter sw = new StringWriter();

		try {
			// jdk自带转换类
			JAXBContext context = JAXBContext.newInstance(obj.getClass());
			// 对象转xml
			Marshaller marshaller = context.createMarshaller();
			// 格式化xml输出
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			// 将对象转换成输出流形式的XML
			marshaller.marshal(obj, sw);

		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return sw.toString();
	}

}
