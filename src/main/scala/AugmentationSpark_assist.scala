object AugmentationSpark_assist {

  val timeOnPage = (x: Seq[Int], timeIgnorance: Int) => {
    x.zip(x.tail).map { case (a, b) => b - a } :+ timeIgnorance
  }

  val sessMask = (x: Seq[Int], breakPoint: Int) => {
    x.map(t => if (t > breakPoint) 1 else 0)
  }

  val  sessNum = (x: Seq[Int]) => {
    x.foldLeft((Seq.empty[Int], 0)) { case ((acc, sessNum), t) =>
      if (t == 1) (acc :+ (sessNum + 1), sessNum + 1)
      else (acc :+ sessNum, sessNum)
    }._1
  }

  val chainMask = (x: Seq[Int]) => {
    if (x.length == 1) Seq(false)
    else (x.init zip x.tail).map { case (a, b) => a == b } :+ false
  }

  val toCategorical = (path: Seq[String], sess: Seq[Int], time: Seq[Int],
                    sessDict: Map[String, Map[String, Int]],
                    topDict: Map[String, Map[String, Int]]) => {
    path.zip(sess).zip(time).map { case ((e, s), t) =>
      val sessCat = if (s < sessDict(e)("25%")) "sess25%"
      else if (s < sessDict(e)("50%")) "sess50%"
      else if (s < sessDict(e)("75%")) "sess75%"
      else "sess100%"

      val topCat = if (t < topDict(e)("25%")) "pageT25%"
      else if (t < topDict(e)("50%")) "pageT50%"
      else if (t < topDict(e)("75%")) "pageT75%"
      else "pageT100%"

      s"$e _>>_ $sessCat _>>_ $topCat"
    }
  }


}

