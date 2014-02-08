package com.csvfile.sorter.samples.serialize;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;


/**
* ファイルにデータを読み書きするときのシリアライズのパフォーマンス改善クラス
*
* ファイルへの読み・書き時のパフォーマンスを改善するために、Externalizableを実装した本クラスを用意し、
* ファイルへの読み・書き処理を独自定義する。
*
* ファイルへ書き込むデータは List<String> の型のものだけを想定してい
*
*/
public class RowData implements Externalizable {

    // 読み・書き対象のデータ
	private List<String> data = null;

    /*
     * Externalizable インタフェースを実装しているクラスは、引数なしコンストラクタは定義する必要がある。
     * Externalizable オブジェクトが直列化復元されるときは、最初に引数なしコンストラクタを呼び出すことによって構築される必要があるため。
     * 引数なしコンストラクタがない場合、直列化と直列化復元は実行時に失敗する。
     *
     * 本クラスでは直列化、直列化復元時に任意の処理はないため、親クラスのコンストラクタを呼ぶだけとする。
     */
	public RowData( List<String> data ) {
		this.data = data;
	}

	/**
     * ストリームからオブジェクトを読み込む処理
     *
     * 読み込み手順は、以下の順で読み込む
     *
     * １．データの個数（列数）
     * ２．実データ
     *
     * １．でデータの個数を読み込み、列数を取得する。
     * ２．は１．で取得した列数分のループを行い、メンバ変数へ格納する。
     *
     * @param in : ストリームから読み込んだオブジェクト
     */
	@Override
	public void writeExternal( ObjectOutput out) throws IOException {
		
		out.writeInt(data.size());
		
		for( String value : data ) {
			if( null != value ) {
				out.writeBoolean(true);
				out.writeUTF(value);
			} else {
				out.writeBoolean(false);                 
			}
		}
	}

	 /**
     * ストリームへオブジェクトを書き込む処理
     *
     * 書き込み手順は、以下の順で書き込む
     *
     * １．データの個数（列数）
     * ２．実データ
     *
     * １．でデータの個数を書き込んでいるが、読み込み用として書き込んでいる。
     * ２．で、null判定を行っているが、null値を書き込むと例外が発生するため、書き込む前に値の判定を行い、
     * null値の場合は真偽値の偽を、null値でない場合は真偽値の真とデータを書き込む。
     *
     * @param out : ストリームへ書き込むオブジェクト
     */
	@Override
	public void readExternal( ObjectInput in) throws IOException, ClassNotFoundException {
		
		int columnCount = in.readInt();
		
		data = new ArrayList<String>( columnCount );
		
		for( int i = 0; i < columnCount; i++) {
			if( in.readBoolean() ) {
				data.add(in.readUTF());
			} else {
				data.add(null);
			}
		}
	}
	
	public List<String> getData() {
		return this.data;
	}
}