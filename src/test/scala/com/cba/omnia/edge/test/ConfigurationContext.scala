package com.cba.omnia.edge
package test

import org.apache.hadoop.conf.Configuration

trait ConfigurationContext {
  lazy val conf: Configuration = new Configuration
}
