package org.apache.sysds.runtime.transform.tokenize;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.matrix.data.FrameBlock;

public class TokenizerPostPosition implements TokenizerPost{
    @Override
    public FrameBlock tokenizePost(Tokenizer.DocumentsToTokenList tl, FrameBlock out) {
        tl.forEach((key, tokenList) -> {
            for (Tokenizer.Token token: tokenList) {
                String position = String.valueOf(token.startIndex);
                String[] row = {key, position, token.textToken};
                out.appendRow(row);
            }
        });

        return out;
    }

    @Override
    public Types.ValueType[] getOutSchema() {
        // Not sure why INT64 is required here, but CP Instruction fails otherwise
        return new Types.ValueType[]{Types.ValueType.STRING, Types.ValueType.INT64, Types.ValueType.STRING};
    }
}
