package com.cba.omnia.edge
package source.template


import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tap.hadoop.TemplateTap
import cascading.tap.hadoop.Hfs
import cascading.tuple.Tuple
import cascading.tuple.Fields

import com.twitter.scalding._
import com.twitter.scalding.typed._

import hdfs.Hdfs.mkdirs
import hdfs.HdfsString._

/**
 * This is a primitive trait to mixin in to enable cascading's TemplateTap.
 *
 * By specifying `template` (a string format) and `templateFields`
 * (as the list of fields to call the template with), multiple
 * output directories (bins) will be created, once for each unique template
 * value, and using the underlying tap implementation data will be
 * partitioned into these sub-directories based upon the values in
 * the specified fields.
 *
 * Note that this trait has a number of limitations, specifically that
 * it uses the template for writing, but currently does not support
 * using GlobHfs or similar for partioned reading (this is the same
 * limitation as using TemplateTap in cascading has).
 *
 * Note also you need to be deliberate about how you use this source
 * and depending how you construct your job the partitioning may run
 * in the map or reduce phase. Often it is desirable to have it run
 * during the reduce phase, this will happen if there is a group by
 * on the fields used in the templates, if your job does not naturally
 * have this structure you can add the following to your job:
 *
 * {{{
 *   pipe.groupBy(templateFields) { _.pass }
 * }}}
 *
 * This implementation is based upon a suggested approach for scalding
 * discussed in https://github.com/twitter/scalding/issues/484.
 */
trait TemplateSource extends FileSource {
  /** The template to use to partition sources, based on java.util.Formatter */
  def template: String

  /** The fields to populate the template with */
  def templateFields: Fields

  override def createTap(readOrWrite: AccessMode)(implicit mode : Mode): Tap[_,_,_] =
    (mode, readOrWrite) match {
      case (hdfsMode @ Hdfs(_, _), Read) =>
        createHdfsReadTap(hdfsMode)
      case (Hdfs(_, c), Write) =>
        mkdirs(hdfsWritePath.toPath).run(c)
        val hfs = new Hfs(hdfsScheme, hdfsWritePath, SinkMode.REPLACE)
        new TemplateTap(hfs, template, templateFields)
      case (_, _) =>
        super.createTap(readOrWrite)(mode)
    }
}


/**
 * Delimited text that is partitioned into bins based upon `template` and
 * `templateFields`.
 */
case class TemplateCsv(
  path: String,
  override val template: String,
  override val templateFields: Fields,
  override val separator: String = ",",
  override val fields: Fields = Fields.ALL,
  override val skipHeader: Boolean = false,
  override val writeHeader: Boolean = false,
  override val quote: String ="\""
) extends FixedPathSource(path)
     with DelimitedScheme
     with TemplateSource
