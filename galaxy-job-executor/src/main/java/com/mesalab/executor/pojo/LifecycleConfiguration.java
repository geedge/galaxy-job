package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "LifecycleConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
public class LifecycleConfiguration {

	@XmlElement(name="Rule")
	private Rule rule = new Rule();

	@NoArgsConstructor
	@AllArgsConstructor
	public static class Rule {
		@XmlElement(name="ID")
		private String id = "1";
		@XmlElement(name="Prefix")
		private String prefix = "";
		@XmlElement(name="Status")
		private String status = "Enabled";

		private Expiration expiration;
		@XmlElement(name="Expiration")
		public Expiration getExpiration() {
			return expiration;
		}
		public void setExpiration(Expiration expiration) {
			this.expiration = expiration;
		}
	}
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Expiration {

		private String days;

		@XmlElement(name="Days")
		public String getDays() {
			return days;
		}
		public void setDays(String days) {
			this.days = days;
		}

	}

	public Rule getRule() {
		return rule;
	}

	public void setRule(Rule rule) {
		this.rule = rule;
	}
}
